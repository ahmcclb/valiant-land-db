import psycopg2
import psycopg2.extras
from psycopg2 import sql
from supabase import create_client, Client
import hashlib
import os
from datetime import datetime
from typing import List, Dict, Optional
import json
import logging
import sys


def get_base_dir():
    """Get the base directory - works for both script and frozen EXE"""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_dir()

# Setup file logging for EXE debugging
log_file = os.path.join(get_base_dir(), 'sync_debug.log')
logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Also log to console if available (development)
if not getattr(sys, 'frozen', False):
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logger.addHandler(console)

def load_config():
    """Load config from JSON file"""
    # CRITICAL: When frozen, config.json is next to EXE (persistent), not in PyInstaller temp folder
    if getattr(sys, 'frozen', False):
        config_path = os.path.join(os.path.dirname(sys.executable), 'config.json')
    else:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')
    
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            'supabase_url': os.getenv('SUPABASE_URL'),
            'supabase_key': os.getenv('SUPABASE_KEY'),
            'sync_mode': os.getenv('SYNC_MODE', 'manual')
        }

def serialize_datetime(obj):
    """Convert datetime objects to ISO format strings for JSON serialization"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def prepare_record_for_supabase(record: dict) -> dict:
    """Convert a database record dict to JSON-serializable format"""
    clean_record = {}
    for key, value in record.items():
        if isinstance(value, datetime):
            clean_record[key] = value.isoformat()
        elif value is None:
            clean_record[key] = None
        else:
            clean_record[key] = value
    return clean_record

class ValiantLandSync:
    def __init__(self, local_config: dict = None, supabase_url: str = None, supabase_key: str = None):
        config = load_config()
        self.local_config = local_config or config.get('local_db')
        self.supabase: Client = create_client(
            supabase_url or config['supabase_url'], 
            supabase_key or config['supabase_key']
        )
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.static_path = os.path.join(self.base_dir, 'static')
        self.uploads_path = os.path.join(self.static_path, 'uploads')
        self.photos_path = os.path.join(self.uploads_path, 'photos')
        self.documents_path = os.path.join(self.uploads_path, 'documents')
        self.sync_batch_size = int(config.get('sync_batch_size', 250))
        
    def get_local_connection(self):
        return psycopg2.connect(**self.local_config)

    def _parse_sync_dt(self, value):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except Exception:
                return None
        return None

    def _normalize_reference_row(self, row: dict) -> dict:
        normalized = {}
        for key, value in dict(row).items():
            if isinstance(value, datetime):
                normalized[key] = value.isoformat()
            else:
                normalized[key] = value
        return normalized

    def _reference_rows_differ(self, left: dict, right: dict) -> bool:
        return self._normalize_reference_row(left) != self._normalize_reference_row(right)

    def _upsert_local_row(self, cursor, table_name: str, pk_field: str, row: dict):
        row = dict(row)
        columns = list(row.keys())

        insert_sql = sql.SQL("""
            INSERT INTO {table} ({columns})
            VALUES ({values})
            ON CONFLICT ({pk}) DO UPDATE
            SET {updates}
        """).format(
            table=sql.Identifier(table_name),
            columns=sql.SQL(', ').join(sql.Identifier(col) for col in columns),
            values=sql.SQL(', ').join(sql.Placeholder() for _ in columns),
            pk=sql.Identifier(pk_field),
            updates=sql.SQL(', ').join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                for col in columns if col != pk_field
            )
        )

        cursor.execute(insert_sql, [row[col] for col in columns])

    def _sync_reference_rows(self, cursor, table_name: str, cloud_table: str, pk_field: str,
                             direction: str, local_where: str = None,
                             cloud_filter: dict = None) -> int:
        local_sql = f"SELECT * FROM {table_name}"
        if local_where:
            local_sql += f" WHERE {local_where}"
        cursor.execute(local_sql)
        local_rows = {row[pk_field]: dict(row) for row in cursor.fetchall()}

        query = self.supabase.table(cloud_table).select('*')
        if cloud_filter:
            for field, value in cloud_filter.items():
                query = query.eq(field, value)
        response = query.execute()
        cloud_rows = {row[pk_field]: dict(row) for row in (response.data or [])}

        synced_count = 0
        all_ids = sorted(set(local_rows.keys()) | set(cloud_rows.keys()))

        for row_id in all_ids:
            local_row = local_rows.get(row_id)
            cloud_row = cloud_rows.get(row_id)

            if local_row and not cloud_row:
                if direction in ('to_cloud', 'bidirectional'):
                    self.supabase.table(cloud_table).upsert(
                        prepare_record_for_supabase(local_row)
                    ).execute()
                    synced_count += 1
                continue

            if cloud_row and not local_row:
                if direction in ('from_cloud', 'bidirectional'):
                    self._upsert_local_row(cursor, table_name, pk_field, cloud_row)
                    synced_count += 1
                continue

            if local_row and cloud_row and self._reference_rows_differ(local_row, cloud_row):
                if direction == 'to_cloud':
                    self.supabase.table(cloud_table).upsert(
                        prepare_record_for_supabase(local_row)
                    ).execute()
                    synced_count += 1
                    continue

                if direction == 'from_cloud':
                    self._upsert_local_row(cursor, table_name, pk_field, cloud_row)
                    synced_count += 1
                    continue

                local_dt = self._parse_sync_dt(local_row.get('modified_at'))
                cloud_dt = self._parse_sync_dt(cloud_row.get('modified_at'))

                if local_dt and cloud_dt:
                    if local_dt > cloud_dt:
                        self.supabase.table(cloud_table).upsert(
                            prepare_record_for_supabase(local_row)
                        ).execute()
                        synced_count += 1
                    elif cloud_dt > local_dt:
                        self._upsert_local_row(cursor, table_name, pk_field, cloud_row)
                        synced_count += 1
                elif local_dt and not cloud_dt:
                    self.supabase.table(cloud_table).upsert(
                        prepare_record_for_supabase(local_row)
                    ).execute()
                    synced_count += 1
                elif cloud_dt and not local_dt:
                    self._upsert_local_row(cursor, table_name, pk_field, cloud_row)
                    synced_count += 1

        return synced_count
        
    def _get_cloud_mail_image_paths(self, p_id: int) -> set:
        """Return known mail-image paths/filenames for a property from Supabase properties."""
        paths = set()
        try:
            response = self.supabase.table('properties')\
                .select('p_mail_image_1,p_mail_image_2')\
                .eq('p_id', p_id)\
                .limit(1)\
                .execute()

            if response.data:
                row = response.data[0]
                for key in ('p_mail_image_1', 'p_mail_image_2'):
                    value = row.get(key)
                    if value:
                        normalized = str(value).replace('\\', '/')
                        paths.add(normalized)
                        paths.add(os.path.basename(normalized))
        except Exception as e:
            logger.debug(f"Warning: Could not fetch cloud mail image paths for property {p_id}: {e}")

        return paths

    def _delete_local_file_and_prune(self, relative_path: str):
        """Delete a local file by app-relative path and remove empty parent p_id folder if possible."""
        try:
            if not relative_path:
                return

            normalized = relative_path.replace('\\', '/')
            absolute_path = os.path.normpath(os.path.join(self.static_path, *normalized.split('/')))

            if os.path.exists(absolute_path) and os.path.isfile(absolute_path):
                os.remove(absolute_path)
                logger.debug(f"Deleted local file: {absolute_path}")

            parent_dir = os.path.dirname(absolute_path)
            if os.path.isdir(parent_dir) and not os.listdir(parent_dir):
                os.rmdir(parent_dir)
                logger.debug(f"Deleted empty folder: {parent_dir}")

        except Exception as e:
            logger.debug(f"Warning: Could not delete local file/folder for {relative_path}: {e}")


    def _get_record_file_path(self, cursor, table_name: str, record_id: int) -> Optional[str]:
        """Return file_path for a property_photos/property_documents record before deletion."""
        try:
            if table_name == 'property_photos':
                cursor.execute("SELECT file_path FROM property_photos WHERE photo_id = %s", (record_id,))
            elif table_name == 'property_documents':
                cursor.execute("SELECT file_path FROM property_documents WHERE doc_id = %s", (record_id,))
            else:
                return None

            row = cursor.fetchone()
            return row['file_path'] if row and row.get('file_path') else None

        except Exception as e:
            logger.debug(f"Warning: Could not fetch file_path for {table_name} {record_id}: {e}")
            return None
    
    def sync_reference_tables(self, direction: str = 'bidirectional') -> dict:
        """Sync reference tables only when rows are missing or changed."""
        stats = {
            'statuses_synced': 0,
            'tags_synced': 0,
            'companies_synced': 0,
            'templates_synced': 0
        }

        conn = self.get_local_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            stats['statuses_synced'] = self._sync_reference_rows(
                cursor=cursor,
                table_name='statuses',
                cloud_table='statuses',
                pk_field='status_id',
                direction=direction
            )

            stats['tags_synced'] = self._sync_reference_rows(
                cursor=cursor,
                table_name='tags',
                cloud_table='tags',
                pk_field='tag_id',
                direction=direction
            )

            stats['companies_synced'] = self._sync_reference_rows(
                cursor=cursor,
                table_name='companies',
                cloud_table='companies',
                pk_field='c_id',
                direction=direction,
                local_where='c_id = 1',
                cloud_filter={'c_id': 1}
            )

            stats['templates_synced'] = self._sync_reference_rows(
                cursor=cursor,
                table_name='document_templates',
                cloud_table='document_templates',
                pk_field='template_id',
                direction=direction,
                local_where='is_active = TRUE',
                cloud_filter={'is_active': True}
            )

            conn.commit()
            return stats

        finally:
            cursor.close()
            conn.close()
    
    def sync_database(self, direction: str = 'from_cloud') -> dict:
        """
        Safer session-oriented sync:
        - start of session: from_cloud
        - end of session: to_cloud
        - bidirectional is allowed, but pull happens first, then push
        """
        stats = {
            'properties_pushed': 0,
            'properties_pulled': 0,
            'owners_pushed': 0,
            'owners_pulled': 0,
            'links_pushed': 0,
            'links_pulled': 0,
            'photos_pulled': 0,
            'documents_pulled': 0,
            'files_uploaded': 0,
            'files_downloaded': 0,
            'conflicts': [],
            'errors': []
        }

        try:
            ref_stats = self.sync_reference_tables(direction)       
            logger.debug(f"Reference tables synced: {ref_stats}")
            stats.update(ref_stats)

            if direction == 'from_cloud':
                pull_stats = self._pull_from_cloud()
                stats.update(pull_stats)

                file_stats = self.sync_files('from_cloud')
                stats['files_downloaded'] = file_stats.get('downloaded', 0)
                if file_stats.get('failed'):
                    stats['errors'].extend(file_stats['failed'])

            elif direction == 'to_cloud':
                push_stats = self._push_to_cloud()
                stats.update(push_stats)

                file_stats = self.sync_files('to_cloud')
                stats['files_uploaded'] = file_stats.get('uploaded', 0)
                if file_stats.get('failed'):
                    stats['errors'].extend(file_stats['failed'])

            elif direction == 'bidirectional':
                # Safer order than before: pull first, then push
                pull_stats = self._pull_from_cloud()
                stats.update(pull_stats)

                file_pull_stats = self.sync_files('from_cloud')
                stats['files_downloaded'] = file_pull_stats.get('downloaded', 0)
                if file_pull_stats.get('failed'):
                    stats['errors'].extend(file_pull_stats['failed'])

                push_stats = self._push_to_cloud()
                stats.update(push_stats)

                file_push_stats = self.sync_files('to_cloud')
                stats['files_uploaded'] = file_push_stats.get('uploaded', 0)
                if file_push_stats.get('failed'):
                    stats['errors'].extend(file_push_stats['failed'])

            else:
                raise ValueError(f"Unknown sync direction: {direction}")

        except Exception as e:
            import traceback
            error_msg = str(e)
            logger.debug("SYNC ERROR: %s", error_msg)
            logger.debug(traceback.format_exc())
            stats['errors'].append(error_msg)

        return stats
    
    def _sync_single_owner(self, or_id: int, cursor) -> bool:
        """Sync a single owner by ID - used for on-demand FK resolution"""
        try:
            cursor.execute("SELECT * FROM owners WHERE or_id = %s", (or_id,))
            owner = cursor.fetchone()
            
            if not owner:
                logger.debug(f"Warning: Owner {or_id} not found in local DB")
                return False
            
            owners_columns = {
                'or_id', 'o_type', 'or_fname', 'or_lname', 'or_email', 'or_phone', 'or_fax',
                'o_fname', 'o_lname', 'o_2fname', 'o_2lname', 'o_3fname', 'o_3lname',
                'o_4fname', 'o_4lname', 'o_5fname', 'o_5lname', 'o_company', 'o_multiple',
                'o_other_owners', 'or_m_address', 'or_m_address2', 'or_m_city',
                'or_m_state', 'or_m_zip', 'modified_at', 'sync_status', 'sync_version',
                'last_sync_at', 'sync_source'
            }
            
            full_dict = dict(owner)
            owner_dict = {k: v for k, v in full_dict.items() if k in owners_columns}
            owner_dict = {k: v for k, v in owner_dict.items() if v is not None and v != ''}
            
            for required in ['or_id', 'sync_status', 'sync_version']:
                if required not in owner_dict and required in full_dict:
                    owner_dict[required] = full_dict[required]
            
            owner_dict = prepare_record_for_supabase(owner_dict)
            owner_dict['sync_status'] = 'synced'
            owner_dict['last_sync_at'] = datetime.now().isoformat()
            owner_dict['sync_source'] = 'local'
            
            response = self.supabase.table('owners').upsert(owner_dict).execute()
            
            if response.data:
                cursor.execute("""
                    UPDATE owners 
                    SET sync_status = 'synced', 
                        last_sync_at = NOW(),
                        sync_version = sync_version + 1
                    WHERE or_id = %s
                """, (or_id,))
                return True
                
        except Exception as e:
            logger.debug(f"Error syncing single owner {or_id}: {e}")
            
        return False
    
    def _push_to_cloud(self) -> dict:
        """Push pending local changes to Supabase - Only changed owners first, then properties"""
        stats = {'properties_pushed': 0, 'owners_pushed': 0, 'errors': []}
        
        conn = self.get_local_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            cursor.execute("""
                SELECT o.* 
                FROM owners o
                WHERE o.sync_status = 'pending'
                   OR (
                        o.last_sync_at IS NOT NULL
                        AND o.modified_at IS NOT NULL
                        AND o.modified_at > o.last_sync_at
                   )
                ORDER BY o.or_id
                LIMIT %s
            """, (self.sync_batch_size,))
            
            pending_owners = cursor.fetchall()
            logger.debug(f"Found {len(pending_owners)} owners needing sync (not 58K!)")
            
            owners_columns = {
                'or_id', 'o_type', 'or_fname', 'or_lname', 'or_email', 'or_phone', 'or_fax',
                'o_fname', 'o_lname', 'o_2fname', 'o_2lname', 'o_3fname', 'o_3lname',
                'o_4fname', 'o_4lname', 'o_5fname', 'o_5lname', 'o_company', 'o_multiple',
                'o_other_owners', 'or_m_address', 'or_m_address2', 'or_m_city',
                'or_m_state', 'or_m_zip', 'modified_at', 'sync_status', 'sync_version',
                'last_sync_at', 'sync_source'
            }
            
            failed_owners = []
            for owner in pending_owners:
                full_dict = dict(owner)
                owner_dict = {k: v for k, v in full_dict.items() if k in owners_columns}
                owner_dict = {k: v for k, v in owner_dict.items() if v is not None and v != ''}
                
                for required in ['or_id', 'sync_status', 'sync_version']:
                    if required not in owner_dict and required in full_dict:
                        owner_dict[required] = full_dict[required]
                
                owner_dict = prepare_record_for_supabase(owner_dict)
                owner_dict['sync_status'] = 'synced'
                owner_dict['last_sync_at'] = datetime.now().isoformat()
                owner_dict['sync_source'] = 'local'
                
                try:
                    response = self.supabase.table('owners').upsert(owner_dict).execute()
                    
                    if response.data:
                        cursor.execute("""
                            UPDATE owners 
                            SET sync_status = 'synced', 
                                last_sync_at = NOW(),
                                sync_version = sync_version + 1
                            WHERE or_id = %s
                        """, (owner['or_id'],))
                        
                        stats['owners_pushed'] += 1
                        
                except Exception as owner_error:
                    error_str = str(owner_error)
                    logger.debug(f"Error syncing owner {owner['or_id']}: {error_str}")
                    stats['errors'].append(f"or_id {owner['or_id']}: {error_str}")
                    failed_owners.append(owner['or_id'])
                    continue
            
            conn.commit()
            logger.debug(f"Synced {stats['owners_pushed']} owners, {len(failed_owners)} failed")
            
            cursor.execute("""
                SELECT p.* 
                FROM properties p
                WHERE p.sync_status = 'pending'
                   OR (
                        p.last_sync_at IS NOT NULL
                        AND p.modified_at IS NOT NULL
                        AND p.modified_at > p.last_sync_at
                   )
                ORDER BY p.p_id
                LIMIT %s
            """, (self.sync_batch_size,))
            
            pending_props = cursor.fetchall()
            logger.debug(f"Processing {len(pending_props)} properties")
            
            properties_columns = {
                'p_id', 'or_id', 'p_status_id', 'p_state', 'p_longstate', 'p_county',
                'p_address', 'p_city', 'p_zip', 'p_apn', 'p_acres', 'p_sqft', 
                'p_terrain', 'p_short_legal', 'p_zoning', 'p_use', 'p_use_code',
                'p_use_description', 'p_restrictions', 'p_flood', 'p_flood_description',
                'p_environmental', 'p_price', 'p_liens', 'p_back_tax', 'p_base_tax',
                'p_comp_market_value', 'p_county_market_value', 'p_county_assessed_value',
                'p_sale_price', 'p_hoa', 'p_impact_fee', 'p_min_acceptable_offer',
                'p_max_offer_amount', 'p_est_value', 'p_improvements', 'p_power',
                'p_access', 'p_waste_system_requirement', 'p_water_system_requirement',
                'p_survey', 'p_owned', 'p_aquired', 'p_listed', 'p_agent_name',
                'p_agent_phone', 'p_viable', 'p_m_date', 'p_offer_accept_date',
                'p_contract_expires_date', 'p_purchased_on', 'p_purchase_amount',
                'p_purchase_closing_costs', 'p_closing_company_name_purchase',
                'p_sold_on', 'p_buyer', 'p_sold_amount', 'p_sold_closing_costs',
                'p_profit', 'p_closing_company_name_sale', 'p_plat_map_link',
                'p_comments', 'p_note', 'p_betty_score', 'p_create_time',
                'p_last_updated', 'p_status_last_updated', 'p_last_sold_date',
                'p_last_sold_amount', 'p_last_transaction_date', 'p_last_transaction_doc_type',
                'modified_at', 'sync_status', 'sync_version', 'last_sync_at', 'sync_source',
                'p_mail_image_1', 'p_mail_image_2'
            }
            
            for prop in pending_props:
                full_dict = dict(prop)
                prop_dict = {k: v for k, v in full_dict.items() if k in properties_columns}
                prop_dict = {k: v for k, v in prop_dict.items() if v is not None and v != ''}
                
                numeric_fields = ['p_price', 'p_comp_market_value', 'p_county_market_value', 
                                 'p_county_assessed_value', 'p_sale_price', 'p_hoa', 
                                 'p_impact_fee', 'p_min_acceptable_offer', 'p_max_offer_amount',
                                 'p_est_value', 'p_purchase_amount', 'p_purchase_closing_costs',
                                 'p_sold_amount', 'p_sold_closing_costs', 'p_profit', 
                                 'p_last_sold_amount', 'p_back_tax', 'p_liens', 'p_base_tax',
                                 'p_acres']

                for field in numeric_fields:
                    if field in prop_dict and prop_dict[field] is not None:
                        try:
                            val = float(prop_dict[field])
                            prop_dict[field] = format(val, '.2f')
                        except (ValueError, TypeError) as e:
                            logger.debug(f"Warning: Could not convert {field} value '{prop_dict[field]}' for property {prop['p_id']}: {e}")
                            prop_dict[field] = None

                if 'p_sqft' in prop_dict and prop_dict['p_sqft'] is not None:
                    try:
                        prop_dict['p_sqft'] = int(float(prop_dict['p_sqft']))
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Warning: Could not convert p_sqft value '{prop_dict['p_sqft']}' for property {prop['p_id']}: {e}")
                        prop_dict['p_sqft'] = None
                
                for required in ['p_id', 'or_id', 'sync_status', 'sync_version']:
                    if required not in prop_dict and required in full_dict:
                        prop_dict[required] = full_dict[required]
                
                prop_dict = prepare_record_for_supabase(prop_dict)
                prop_dict['sync_status'] = 'synced'
                prop_dict['last_sync_at'] = datetime.now().isoformat()
                prop_dict['sync_source'] = 'local'
                
                try:
                    response = self.supabase.table('properties').upsert(prop_dict).execute()
                    
                    if response.data:
                        cursor.execute("""
                            UPDATE properties 
                            SET sync_status = 'synced', 
                                last_sync_at = NOW(),
                                sync_version = sync_version + 1
                            WHERE p_id = %s
                        """, (prop['p_id'],))
                        
                        stats['properties_pushed'] += 1
                        
                except Exception as prop_error:
                    error_str = str(prop_error)
                    
                    if 'foreign key' in error_str.lower() or 'violates foreign' in error_str.lower() or 'foreign key constraint' in error_str.lower():
                        logger.debug(f"FK error for property {prop['p_id']}, attempting to sync owner {prop['or_id']}...")
                        
                        if self._sync_single_owner(prop['or_id'], cursor):
                            try:
                                response = self.supabase.table('properties').upsert(prop_dict).execute()
                                if response.data:
                                    cursor.execute("""
                                        UPDATE properties 
                                        SET sync_status = 'synced', 
                                            last_sync_at = NOW(),
                                            sync_version = sync_version + 1
                                        WHERE p_id = %s
                                    """, (prop['p_id'],))
                                    stats['properties_pushed'] += 1
                                    logger.debug(f"Retry successful for property {prop['p_id']}")
                                    continue
                            except Exception as retry_error:
                                error_str = str(retry_error)
                                logger.debug(f"Retry failed for property {prop['p_id']}: {error_str}")
                    
                    logger.debug(f"Error syncing property {prop['p_id']}: {error_str}")
                    stats['errors'].append(f"p_id {prop['p_id']}: {error_str}")
                    continue
            
            conn.commit()
          
            # Sync property links for synced properties
            if pending_props:
                p_ids = [p['p_id'] for p in pending_props]
                placeholders = ','.join(['%s'] * len(p_ids))
                cursor.execute(f"""
                    SELECT * FROM property_links 
                    WHERE p_id IN ({placeholders})
                """, p_ids)
                local_links = cursor.fetchall()
                
                for link in local_links:
                    try:
                        link_dict = dict(link)
                        # FIX: Serialize datetime fields for JSON
                        if isinstance(link_dict.get('added_date'), datetime):
                            link_dict['added_date'] = link_dict['added_date'].isoformat()
                        if isinstance(link_dict.get('modified_at'), datetime):
                            link_dict['modified_at'] = link_dict['modified_at'].isoformat()
                            
                        # Check if exists in cloud
                        cloud_check = self.supabase.table('property_links')\
                            .select('link_id')\
                            .eq('link_id', link_dict['link_id'])\
                            .execute()
                        
                        link_record = {
                            'link_id': link_dict['link_id'],
                            'p_id': link_dict['p_id'],
                            'url': link_dict['url'],
                            'description': link_dict.get('description', ''),
                            'added_date': link_dict.get('added_date', datetime.now().isoformat()),
                            'modified_at': datetime.now().isoformat(),
                            'sync_status': 'synced',
                            'last_sync_at': datetime.now().isoformat(),
                            'sync_version': 1
                        }
                        
                        if not cloud_check.data:
                            self.supabase.table('property_links').insert(link_record).execute()
                            stats['links_pushed'] = stats.get('links_pushed', 0) + 1
                            logger.debug(f"Pushed link {link_dict['link_id']} for property {link_dict['p_id']}")
                        else:
                            self.supabase.table('property_links')\
                                .upsert(link_record)\
                                .execute()
                            stats['links_pushed'] = stats.get('links_pushed', 0) + 1
                            logger.debug(f"Upserted link {link_dict['link_id']} for property {link_dict['p_id']}")
                                
                    except Exception as link_error:
                        logger.debug(f"Warning: Could not sync link {link.get('link_id')}: {link_error}")
                        continue            
            
            logger.debug(f"Completed: {stats['properties_pushed']} properties synced")
            
        except Exception as e:
            conn.rollback()
            logger.debug(f"PUSH ERROR: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            raise e
        finally:
            cursor.close()
            conn.close()
            
        return stats
    
    def _sync_property_files_to_cloud(self, cursor, p_ids):
        """FIX: Push property_photos and property_documents records to cloud"""
        try:
            # Sync photos
            placeholders = ','.join(['%s'] * len(p_ids))
            cursor.execute(f"""
                SELECT * FROM property_photos 
                WHERE p_id IN ({placeholders})
            """, p_ids)
            photos = cursor.fetchall()

            for photo in photos:
                photo_dict = dict(photo)
                # CRITICAL FIX: Use prepare_record_for_supabase to handle ALL datetime fields
                photo_dict = prepare_record_for_supabase(photo_dict)
                
                try:
                    self.supabase.table('property_photos').upsert(photo_dict).execute()
                    logger.debug(f"Pushed photo record {photo_dict['photo_id']} for property {photo_dict['p_id']}")
                except Exception as e:
                    logger.debug(f"Warning: Could not push photo {photo_dict.get('photo_id')}: {e}")
            
            # Sync documents
            cursor.execute(f"""
                SELECT * FROM property_documents 
                WHERE p_id IN ({placeholders})
            """, p_ids)
            docs = cursor.fetchall()
            
            for doc in docs:
                doc_dict = dict(doc)
                doc_dict = prepare_record_for_supabase(doc_dict)
                # Serialize dates
                if isinstance(doc_dict.get('upload_date'), datetime):
                    doc_dict['upload_date'] = doc_dict['upload_date'].isoformat()
                
                try:
                    self.supabase.table('property_documents').upsert(doc_dict).execute()
                    logger.debug(f"Pushed document record {doc_dict['doc_id']} for property {doc_dict['p_id']}")
                except Exception as e:
                    logger.debug(f"Warning: Could not push document {doc_dict.get('doc_id')}: {e}")
                    
        except Exception as e:
            logger.debug(f"Error syncing property file records: {e}")
    
    def _pull_from_cloud(self) -> dict:
        """Pull changes from Supabase to local - owners first to satisfy FK constraints"""
        stats = {'properties_pulled': 0, 'owners_pulled': 0, 'photos_pulled': 0, 'documents_pulled': 0, 'conflicts': []}

        conn = self.get_local_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            # PHASE 0: Push pending local deletions to cloud first
            cursor.execute("SELECT * FROM sync_deletions WHERE sync_status = 'pending' AND cloud_deleted = FALSE")
            pending_deletions = cursor.fetchall()

            for deletion in pending_deletions:
                table = deletion['table_name']
                record_id = deletion['record_id']

                id_columns = {
                    'properties': 'p_id',
                    'property_links': 'link_id',
                    'property_photos': 'photo_id',
                    'property_documents': 'doc_id'
                }
                id_column = id_columns.get(table, f"{table[:-1]}_id")

                try:
                    self.supabase.table(table).delete().eq(id_column, record_id).execute()
                    cursor.execute('UPDATE sync_deletions SET cloud_deleted = TRUE WHERE deletion_id = %s', (deletion['deletion_id'],))
                except Exception as e:
                    logger.debug(f"Will retry cloud deletion later for {table} {record_id}: {e}")

            conn.commit()

            # PHASE 0.5: Pull deletions from cloud and apply locally
            try:
                cursor.execute("SELECT MAX(last_sync_at) as max_sync FROM properties")
                result = cursor.fetchone()
                last_sync = result['max_sync'] if result and result['max_sync'] else '1970-01-01'

                id_columns = {
                    'properties': 'p_id',
                    'property_links': 'link_id',
                    'property_photos': 'photo_id',
                    'property_documents': 'doc_id'
                }

                for table_name in ['properties', 'property_links', 'property_photos', 'property_documents']:
                    id_column = id_columns[table_name]

                    cloud_deletions = self.supabase.table('sync_deletions')\
                        .select('*')\
                        .eq('table_name', table_name)\
                        .gt('deleted_at', last_sync)\
                        .execute()

                    for deletion in cloud_deletions.data:
                        record_id = deletion['record_id']

                        file_path = None
                        if table_name in ('property_photos', 'property_documents'):
                            file_path = self._get_record_file_path(cursor, table_name, record_id)

                        cursor.execute(f"DELETE FROM {table_name} WHERE {id_column} = %s", (record_id,))

                        if table_name == 'properties':
                            cursor.execute("DELETE FROM file_sync WHERE replace(local_path, '\\\\', '/') LIKE %s", (f"%/p_{record_id}/%",))

                        elif table_name in ('property_photos', 'property_documents') and file_path:
                            normalized_path = file_path.replace('\\', '/')
                            cursor.execute("""
                                DELETE FROM file_sync
                                WHERE replace(local_path, '\\\\', '/') = %s
                                   OR replace(coalesce(cloud_path, ''), '\\\\', '/') = %s
                            """, (normalized_path, deletion.get('cloud_path', '').replace('\\', '/')))
                            self._delete_local_file_and_prune(normalized_path)

                    if cloud_deletions.data:
                        logger.debug(f"Applied {len(cloud_deletions.data)} {table_name} deletions from cloud")

            except Exception as e:
                logger.debug(f"Error pulling deletions from cloud: {e}")

            # PHASE 1: Pull owners from cloud
            cursor.execute("SELECT MAX(last_sync_at) as max_sync FROM owners")
            result = cursor.fetchone()
            owners_last_sync = result['max_sync'] if result and result['max_sync'] else '1970-01-01'

            cloud_owners = self.supabase.table('owners')\
                .select('*')\
                .gt('modified_at', owners_last_sync)\
                .limit(self.sync_batch_size)\
                .execute()

            # EXPLICITLY DEFINE allowed columns for owners - NO sync_status, last_sync_at, sync_source
            allowed_owner_columns = [
                'or_id', 'o_type', 'or_fname', 'or_lname', 'or_email', 'or_phone', 'or_fax',
                'o_fname', 'o_lname', 'o_2fname', 'o_2lname', 'o_3fname', 'o_3lname',
                'o_4fname', 'o_4lname', 'o_5fname', 'o_5lname', 'o_company', 'o_multiple',
                'o_other_owners', 'or_m_address', 'or_m_address2', 'or_m_city',
                'or_m_state', 'or_m_zip', 'modified_at', 'sync_version'
            ]

            for cloud_owner in cloud_owners.data:
                local_id = cloud_owner.get('or_id')
                
                cursor.execute("""
                    SELECT modified_at, sync_status FROM owners WHERE or_id = %s
                """, (local_id,))
                local_record = cursor.fetchone()

                cloud_modified = cloud_owner.get('modified_at')
                if cloud_modified is None:
                    continue
                if isinstance(cloud_modified, str):
                    cloud_modified = datetime.fromisoformat(cloud_modified.replace('Z', '+00:00'))
                
                if local_record and local_record['sync_status'] == 'pending':
                    if cloud_modified > local_record['modified_at']:
                        stats['conflicts'].append({
                            'table': 'owners',
                            'id': local_id,
                            'resolution': 'manual_review_required',
                            'reason': 'local_pending_and_cloud_newer'
                        })
                        logger.debug(f"Conflict detected for owner {local_id}; skipping automatic overwrite")
                        continue
                    else:
                        continue  # local wins, skip cloud overwrite

                # EXPLICITLY extract only allowed columns - GUARANTEED no sync_status
                owner_data = {}
                for col in allowed_owner_columns:
                    if col in cloud_owner:
                        owner_data[col] = cloud_owner[col]
                
                # Ensure required fields exist
                if 'or_id' not in owner_data or owner_data['or_id'] is None:
                    continue  # Skip invalid records
                
                # Build SQL with EXPLICIT column list - NO dynamic filtering
                columns = list(owner_data.keys())
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)
                
                # Build updates for non-PK columns only - EXPLICITLY exclude sync metadata
                update_parts = []
                for col in columns:
                    if col != 'or_id':
                        update_parts.append(f"{col} = EXCLUDED.{col}")
                
                # Add sync metadata updates - ONLY HERE, never from cloud data
                update_parts.extend([
                    "sync_status = 'synced'",
                    "last_sync_at = NOW()",
                    "sync_source = 'cloud'"
                ])
                
                updates_str = ', '.join(update_parts)
                
                values = [owner_data[col] for col in columns]

                cursor.execute(f"""
                    INSERT INTO owners ({columns_str}, sync_status, last_sync_at, sync_source)
                    VALUES ({placeholders}, 'synced', NOW(), 'cloud')
                    ON CONFLICT (or_id) DO UPDATE SET
                    {updates_str}
                """, tuple(values))

                stats['owners_pulled'] += 1

            # PHASE 2: Pull properties from cloud
            cursor.execute("SELECT MAX(last_sync_at) as max_sync FROM properties")
            result = cursor.fetchone()
            props_last_sync = result['max_sync'] if result and result['max_sync'] else '1970-01-01'

            cloud_props = self.supabase.table('properties')\
                .select('*')\
                .gt('modified_at', props_last_sync)\
                .limit(self.sync_batch_size)\
                .execute()

            # EXPLICITLY DEFINE allowed columns for properties - NO sync_status, last_sync_at, sync_source
            allowed_property_columns = [
                'p_id', 'or_id', 'p_status_id', 'p_state', 'p_longstate', 'p_county',
                'p_address', 'p_city', 'p_zip', 'p_apn', 'p_acres', 'p_sqft',
                'p_terrain', 'p_short_legal', 'p_zoning', 'p_use', 'p_use_code',
                'p_use_description', 'p_restrictions', 'p_flood', 'p_flood_description',
                'p_environmental', 'p_price', 'p_liens', 'p_back_tax', 'p_base_tax',
                'p_comp_market_value', 'p_county_market_value', 'p_county_assessed_value',
                'p_sale_price', 'p_hoa', 'p_impact_fee', 'p_min_acceptable_offer',
                'p_max_offer_amount', 'p_est_value', 'p_improvements', 'p_power',
                'p_access', 'p_waste_system_requirement', 'p_water_system_requirement',
                'p_survey', 'p_owned', 'p_aquired', 'p_listed', 'p_agent_name',
                'p_agent_phone', 'p_viable', 'p_m_date', 'p_offer_accept_date',
                'p_contract_expires_date', 'p_purchased_on', 'p_purchase_amount',
                'p_purchase_closing_costs', 'p_closing_company_name_purchase',
                'p_sold_on', 'p_buyer', 'p_sold_amount', 'p_sold_closing_costs',
                'p_profit', 'p_closing_company_name_sale', 'p_plat_map_link',
                'p_comments', 'p_note', 'p_betty_score', 'p_create_time',
                'p_last_updated', 'p_status_last_updated', 'p_last_sold_date',
                'p_last_sold_amount', 'p_last_transaction_date', 'p_last_transaction_doc_type',
                'modified_at', 'sync_version', 'p_mail_image_1', 'p_mail_image_2'
            ]

            for cloud_prop in cloud_props.data:
                local_id = cloud_prop.get('p_id')
                
                cursor.execute("""
                    SELECT modified_at, sync_status FROM properties WHERE p_id = %s
                """, (local_id,))
                local_record = cursor.fetchone()

                cloud_modified = cloud_prop.get('modified_at')
                if cloud_modified is None:
                    continue
                if isinstance(cloud_modified, str):
                    cloud_modified = datetime.fromisoformat(cloud_modified.replace('Z', '+00:00'))
                
                if local_record and local_record['sync_status'] == 'pending':
                    if cloud_modified > local_record['modified_at']:
                        stats['conflicts'].append({
                            'table': 'properties',
                            'id': local_id,
                            'resolution': 'manual_review_required',
                            'reason': 'local_pending_and_cloud_newer'
                        })
                        logger.debug(f"Conflict detected for property {local_id}; skipping automatic overwrite")
                        continue
                    else:
                        continue  # local wins, skip cloud overwrite

                # EXPLICITLY extract only allowed columns - GUARANTEED no sync_status
                prop_data = {}
                for col in allowed_property_columns:
                    if col in cloud_prop:
                        prop_data[col] = cloud_prop[col]
                
                if 'p_id' not in prop_data or prop_data['p_id'] is None:
                    continue
                
                columns = list(prop_data.keys())
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)
                
                update_parts = []
                for col in columns:
                    if col != 'p_id':
                        update_parts.append(f"{col} = EXCLUDED.{col}")
                
                # Add sync metadata updates - ONLY HERE, never from cloud data
                update_parts.extend([
                    "sync_status = 'synced'",
                    "last_sync_at = NOW()",
                    "sync_source = 'cloud'"
                ])
                
                updates_str = ', '.join(update_parts)
                
                values = [prop_data[col] for col in columns]

                cursor.execute(f"""
                    INSERT INTO properties ({columns_str}, sync_status, last_sync_at, sync_source)
                    VALUES ({placeholders}, 'synced', NOW(), 'cloud')
                    ON CONFLICT (p_id) DO UPDATE SET
                    {updates_str}
                """, tuple(values))

                stats['properties_pulled'] += 1

            # PHASE 3: Pull property links from cloud
            try:
                cloud_links = self.supabase.table('property_links')\
                    .select('*')\
                    .execute()

                for link in cloud_links.data or []:
                    try:
                        link_id = link.get('link_id')
                        if not link_id:
                            continue

                        cloud_modified = link.get('modified_at')
                        if isinstance(cloud_modified, str):
                            cloud_modified_dt = datetime.fromisoformat(cloud_modified.replace('Z', '+00:00'))
                            cloud_modified_value = cloud_modified
                        elif isinstance(cloud_modified, datetime):
                            cloud_modified_dt = cloud_modified
                            cloud_modified_value = cloud_modified.isoformat()
                        else:
                            cloud_modified_dt = None
                            cloud_modified_value = datetime.now().isoformat()

                        added_date = link.get('added_date')
                        if isinstance(added_date, datetime):
                            added_date_value = added_date.isoformat()
                        elif isinstance(added_date, str):
                            added_date_value = added_date
                        else:
                            added_date_value = datetime.now().isoformat()

                        cursor.execute("""
                            SELECT modified_at, sync_status
                            FROM property_links
                            WHERE link_id = %s
                        """, (link_id,))
                        local_link = cursor.fetchone()

                        if local_link and local_link.get('sync_status') == 'pending':
                            local_modified = local_link.get('modified_at')
                            if cloud_modified_dt and local_modified and cloud_modified_dt > local_modified:
                                logger.debug(f"Conflict detected for link {link_id}; local pending but cloud newer. Skipping automatic overwrite.")
                            continue

                        should_upsert = False
                        if not local_link:
                            should_upsert = True
                        else:
                            local_modified = local_link.get('modified_at')
                            if cloud_modified_dt and local_modified:
                                should_upsert = cloud_modified_dt > local_modified
                            else:
                                should_upsert = True

                        if should_upsert:
                            cursor.execute("""
                                INSERT INTO property_links
                                (link_id, p_id, url, description, added_date, modified_at, sync_status, last_sync_at, sync_version)
                                VALUES (%s, %s, %s, %s, %s, %s, 'synced', NOW(), %s)
                                ON CONFLICT (link_id) DO UPDATE SET
                                    p_id = EXCLUDED.p_id,
                                    url = EXCLUDED.url,
                                    description = EXCLUDED.description,
                                    added_date = EXCLUDED.added_date,
                                    modified_at = EXCLUDED.modified_at,
                                    sync_status = 'synced',
                                    last_sync_at = NOW(),
                                    sync_version = EXCLUDED.sync_version
                            """, (
                                link_id,
                                link['p_id'],
                                link['url'],
                                link.get('description', ''),
                                added_date_value,
                                cloud_modified_value,
                                link.get('sync_version', 1)
                            ))

                            stats['links_pulled'] = stats.get('links_pulled', 0) + 1

                logger.debug(f"Pulled {stats.get('links_pulled', 0)} new/changed links from cloud")

            except Exception as e:
                logger.debug(f"Error pulling links: {e}")

            # PHASE 4: Pull property photos and documents from cloud
            try:
                self._pull_property_files_from_cloud(cursor, stats)
            except Exception as e:
                logger.debug(f"Error pulling property files from cloud: {e}")

            # Cleanup: Remove successfully synced tombstones older than 7 days
            cursor.execute('''
                DELETE FROM sync_deletions
                WHERE sync_status = 'synced'
                AND deleted_at < NOW() - INTERVAL '7 days'
            ''')

            conn.commit()

        except Exception as e:
            conn.rollback()
            logger.debug(f"PULL ERROR: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            raise e
        finally:
            cursor.close()
            conn.close()

        return stats
        
    def _pull_property_files_from_cloud(self, cursor, stats):
        """Pull property_photos and property_documents records from cloud to local"""
        logger.info(">>> ENTERING _pull_property_files_from_cloud")  # ADD THIS LINE
        try:
            # CRITICAL FIX: Reset sequences first to avoid ID conflicts
            self._reset_sequences(cursor)
            
            # Pull photos - use a very old date if no last_sync (fresh install)
            cursor.execute("SELECT MAX(modified_at) as max_sync FROM property_photos")
            result = cursor.fetchone()
            photos_last_sync = result['max_sync'] if result and result['max_sync'] else '1970-01-01T00:00:00Z'
            
            logger.debug(f"Pulling photos modified after: {photos_last_sync}")
            
            cloud_photos = self.supabase.table('property_photos')\
                .select('*')\
                .gt('modified_at', photos_last_sync)\
                .execute()
            
            logger.debug(f"Found {len(cloud_photos.data)} photos in cloud")
            
            for photo in cloud_photos.data:
                try:
                    if not photo.get('photo_id') or not photo.get('p_id'):
                        continue

                    # Normalize path separators
                    file_path = (photo.get('file_path') or '').replace('\\', '/')
                    cloud_path = (photo.get('cloud_path') or '').replace('\\', '/')
                    p_id = photo['p_id']

                    if not file_path:
                        continue

                    # Skip mail images - check both local property fields and cloud property fields
                    local_mail_paths = set()
                    cursor.execute("""
                        SELECT p_mail_image_1, p_mail_image_2
                        FROM properties
                        WHERE p_id = %s
                    """, (p_id,))
                    prop_row = cursor.fetchone()

                    if prop_row:
                        for key in ('p_mail_image_1', 'p_mail_image_2'):
                            value = prop_row.get(key)
                            if value:
                                normalized = str(value).replace('\\', '/')
                                local_mail_paths.add(normalized)
                                local_mail_paths.add(os.path.basename(normalized))

                    cloud_mail_paths = self._get_cloud_mail_image_paths(p_id)

                    file_name = os.path.basename(file_path)
                    all_mail_paths = local_mail_paths | cloud_mail_paths

                    if file_path in all_mail_paths or file_name in all_mail_paths:
                        logger.debug(f"Skipping mail image in property_photos pull for p_id {p_id}: {file_path}")
                        continue

                    # DEDUPE by logical identity: p_id + file_path
                    cursor.execute("""
                        SELECT photo_id
                        FROM property_photos
                        WHERE p_id = %s AND file_path = %s
                        ORDER BY photo_id DESC
                        LIMIT 1
                    """, (p_id, file_path))
                    existing = cursor.fetchone()

                    if existing:
                        cursor.execute("""
                            UPDATE property_photos
                            SET file_name = %s,
                                upload_date = %s,
                                caption = %s,
                                is_primary = %s,
                                cloud_path = %s,
                                modified_at = %s,
                                sync_status = 'synced',
                                sync_version = COALESCE(sync_version, 1)
                            WHERE p_id = %s AND file_path = %s
                        """, (
                            photo.get('file_name'),
                            photo.get('upload_date'),
                            photo.get('caption'),
                            photo.get('is_primary', False),
                            cloud_path,
                            photo.get('modified_at', datetime.now().isoformat()),
                            p_id,
                            file_path
                        ))
                    else:
                        cursor.execute("""
                            INSERT INTO property_photos
                            (photo_id, p_id, file_path, file_name, upload_date, caption, is_primary, cloud_path, modified_at, sync_status, sync_version)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'synced', 1)
                            ON CONFLICT (photo_id) DO UPDATE SET
                            p_id = EXCLUDED.p_id,
                            file_path = EXCLUDED.file_path,
                            file_name = EXCLUDED.file_name,
                            upload_date = EXCLUDED.upload_date,
                            caption = EXCLUDED.caption,
                            is_primary = EXCLUDED.is_primary,
                            cloud_path = EXCLUDED.cloud_path,
                            modified_at = EXCLUDED.modified_at,
                            sync_status = 'synced'
                        """, (
                            photo['photo_id'],
                            p_id,
                            file_path,
                            photo.get('file_name'),
                            photo.get('upload_date'),
                            photo.get('caption'),
                            photo.get('is_primary', False),
                            cloud_path,
                            photo.get('modified_at', datetime.now().isoformat())
                        ))

                    stats['photos_pulled'] = stats.get('photos_pulled', 0) + 1

                except Exception as e:
                    logger.error(f"Error pulling photo {photo.get('photo_id')}: {e}")
                    continue
            
            # Pull documents with same logic
            cursor.execute("SELECT MAX(modified_at) as max_sync FROM property_documents")
            result = cursor.fetchone()
            docs_last_sync = result['max_sync'] if result and result['max_sync'] else '1970-01-01T00:00:00Z'
            
            cloud_docs = self.supabase.table('property_documents')\
                .select('*')\
                .gt('modified_at', docs_last_sync)\
                .execute()
            
            logger.debug(f"Found {len(cloud_docs.data)} documents in cloud")
            
            for doc in cloud_docs.data:
                try:
                    if not doc.get('doc_id') or not doc.get('p_id'):
                        continue

                    file_path = (doc.get('file_path') or '').replace('\\', '/')
                    cloud_path = (doc.get('cloud_path') or '').replace('\\', '/')
                    p_id = doc['p_id']

                    if not file_path:
                        continue

                    # DEDUPE by logical identity: p_id + file_path
                    cursor.execute("""
                        SELECT doc_id
                        FROM property_documents
                        WHERE p_id = %s AND file_path = %s
                        ORDER BY doc_id DESC
                        LIMIT 1
                    """, (p_id, file_path))
                    existing = cursor.fetchone()

                    if existing:
                        cursor.execute("""
                            UPDATE property_documents
                            SET file_name = %s,
                                doc_type = %s,
                                upload_date = %s,
                                description = %s,
                                cloud_path = %s,
                                modified_at = %s,
                                sync_status = 'synced',
                                sync_version = COALESCE(sync_version, 1)
                            WHERE p_id = %s AND file_path = %s
                        """, (
                            doc.get('file_name'),
                            doc.get('doc_type'),
                            doc.get('upload_date'),
                            doc.get('description'),
                            cloud_path,
                            doc.get('modified_at', datetime.now().isoformat()),
                            p_id,
                            file_path
                        ))
                    else:
                        cursor.execute("""
                            INSERT INTO property_documents
                            (doc_id, p_id, file_path, file_name, doc_type, upload_date, description, cloud_path, modified_at, sync_status, sync_version)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'synced', 1)
                            ON CONFLICT (doc_id) DO UPDATE SET
                            p_id = EXCLUDED.p_id,
                            file_path = EXCLUDED.file_path,
                            file_name = EXCLUDED.file_name,
                            doc_type = EXCLUDED.doc_type,
                            upload_date = EXCLUDED.upload_date,
                            description = EXCLUDED.description,
                            cloud_path = EXCLUDED.cloud_path,
                            modified_at = EXCLUDED.modified_at,
                            sync_status = 'synced'
                        """, (
                            doc['doc_id'],
                            p_id,
                            file_path,
                            doc.get('file_name'),
                            doc.get('doc_type'),
                            doc.get('upload_date'),
                            doc.get('description'),
                            cloud_path,
                            doc.get('modified_at', datetime.now().isoformat())
                        ))

                    stats['documents_pulled'] = stats.get('documents_pulled', 0) + 1

                except Exception as e:
                    logger.error(f"Error pulling document {doc.get('doc_id')}: {e}")
                    continue
            # Cleanup duplicate photos by keeping the newest row for each (p_id, file_path)
            cursor.execute("""
                DELETE FROM property_photos
                WHERE photo_id IN (
                    SELECT photo_id
                    FROM (
                        SELECT photo_id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY p_id, file_path
                                   ORDER BY modified_at DESC NULLS LAST, photo_id DESC
                               ) AS rn
                        FROM property_photos
                    ) ranked
                    WHERE ranked.rn > 1
                )
            """)

            # Cleanup duplicate documents by keeping the newest row for each (p_id, file_path)
            cursor.execute("""
                DELETE FROM property_documents
                WHERE doc_id IN (
                    SELECT doc_id
                    FROM (
                        SELECT doc_id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY p_id, file_path
                                   ORDER BY modified_at DESC NULLS LAST, doc_id DESC
                               ) AS rn
                        FROM property_documents
                    ) ranked
                    WHERE ranked.rn > 1
                )
            """)
            
            # Cleanup duplicate links by keeping the newest row for each (p_id, url)
            cursor.execute("""
                DELETE FROM property_links
                WHERE link_id IN (
                    SELECT link_id
                    FROM (
                        SELECT link_id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY p_id, url
                                   ORDER BY modified_at DESC NULLS LAST, link_id DESC
                               ) AS rn
                        FROM property_links
                    ) ranked
                    WHERE ranked.rn > 1
                )
            """)
            
            # Reset sequences again after all inserts
            self._reset_sequences(cursor)
            
            logger.info(f"Pulled {stats.get('photos_pulled', 0)} photos and {stats.get('documents_pulled', 0)} document records from cloud")
            
        except Exception as e:
            logger.error(f"Error in _pull_property_files_from_cloud: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _reset_sequences(self, cursor):
        """Reset sequences to match current max IDs to prevent insertion conflicts"""
        try:
            cursor.execute("""
                SELECT setval('property_photos_photo_id_seq', COALESCE((SELECT MAX(photo_id) FROM property_photos), 0) + 1, false);
                SELECT setval('property_documents_doc_id_seq', COALESCE((SELECT MAX(doc_id) FROM property_documents), 0) + 1, false);
                SELECT setval('property_links_link_id_seq', COALESCE((SELECT MAX(link_id) FROM property_links), 0) + 1, false);
            """)
            logger.debug("Reset sequences to max IDs")
        except Exception as e:
            logger.error(f"Error resetting sequences: {e}")
    
    def sync_files(self, direction: str = 'to_cloud') -> dict:
        """Sync photos and documents with Supabase Storage"""
        stats = {'uploaded': 0, 'downloaded': 0, 'failed': []}
        
        conn = self.get_local_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Ensure local file_sync table exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_sync (
                local_path TEXT PRIMARY KEY,
                cloud_path TEXT,
                file_hash TEXT,
                modified_at TIMESTAMP DEFAULT NOW(),
                last_sync_at TIMESTAMP,
                sync_status VARCHAR(20) DEFAULT 'pending'
            )
        """)
        conn.commit()
        
        try:
            if direction in ['to_cloud', 'bidirectional']:
                self._upload_files_to_cloud(cursor, stats, conn)
                
            if direction in ['from_cloud', 'bidirectional']:
                self._download_files_from_cloud(cursor, stats, conn)
            
        except Exception as e:
            stats['failed'].append(f"General error: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
        finally:
            cursor.close()
            conn.close()
            
        return stats

    def _upload_files_to_cloud(self, cursor, stats: dict, conn):
        """Upload local files to Supabase Storage and track in Supabase DB"""
        upload_dirs = [
            os.path.join(BASE_DIR, 'static', 'uploads', 'photos'),
            os.path.join(BASE_DIR, 'static', 'uploads', 'documents'),
            os.path.join(BASE_DIR, 'static', 'uploads', 'company')
        ]
        bucket = 'property-files'
        
        for upload_dir in upload_dirs:
            if not os.path.exists(upload_dir):
                continue
                
            for root, dirs, files in os.walk(upload_dir):
                for file in files:
                    local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_path, os.path.join(BASE_DIR, 'static'))
                    # FIX: Normalize to forward slashes immediately
                    relative_path = relative_path.replace('\\', '/')
                    
                    try:
                        file_hash = self._get_file_hash(local_path)
                        
                        # Check local tracking to avoid re-uploading unchanged files
                        cursor.execute("""
                            SELECT file_hash FROM file_sync 
                            WHERE (local_path = %s OR local_path = %s) AND sync_status = 'synced'
                        """, (relative_path, relative_path.replace('/', '\\')))
                        
                        existing = cursor.fetchone()
                        if existing and existing['file_hash'] == file_hash:
                            continue
                        
                        # Extract property ID from filename (format: {p_id}_{filename})
                        p_id = self._extract_property_id_from_filename(file)
                        is_company_file = 'company' in relative_path.split('/')
                        
                        if not p_id and not is_company_file:
                            logger.debug(f"Warning: Could not extract property ID from {file}")
                            continue
                            
                        # Determine file type and set paths
                        is_photo = self._is_photo_file(file)
                        subdir = 'photos' if is_photo else 'documents'
                        cloud_path = f"{datetime.now().year}/{subdir}/{file}"
                        
                        # FIX: Check if this is a mail image (referenced in properties table, not property_photos)
                        is_mail_image = False
                        if p_id and is_photo:
                            cursor.execute("""
                                SELECT 1 FROM properties 
                                WHERE p_id = %s AND (p_mail_image_1 LIKE %s OR p_mail_image_2 LIKE %s)
                            """, (p_id, f'%{file}', f'%{file}'))
                            if cursor.fetchone():
                                is_mail_image = True
                                logger.debug(f"File {file} is a mail image, will upload but not link to property_photos")
                        
                        # Upload to Supabase Storage
                        with open(local_path, 'rb') as f:
                            self.supabase.storage.from_(bucket).upload(
                                cloud_path, 
                                f,
                                {'content-type': 'application/octet-stream', 'upsert': 'true'}
                            )
                        
                        # 1. Update LOCAL tracking
                        cursor.execute("""
                            INSERT INTO file_sync (local_path, cloud_path, file_hash, modified_at, sync_status, last_sync_at)
                            VALUES (%s, %s, %s, NOW(), 'synced', NOW())
                            ON CONFLICT (local_path) DO UPDATE SET
                            cloud_path = EXCLUDED.cloud_path,
                            file_hash = EXCLUDED.file_hash,
                            sync_status = 'synced',
                            last_sync_at = NOW()
                        """, (relative_path, cloud_path, file_hash))
                        
                        # 2. Insert into SUPABASE file_sync table (for other PCs to see)
                        try:
                            file_sync_record = {
                                'local_path': relative_path,
                                'cloud_path': cloud_path,
                                'file_hash': file_hash,
                                'modified_at': datetime.now().isoformat(),
                                'last_sync_at': datetime.now().isoformat(),
                                'sync_status': 'synced'
                            }
                            self.supabase.table('file_sync').upsert(file_sync_record).execute()
                        except Exception as e:
                            logger.debug(f"Warning: Could not insert file_sync to Supabase: {e}")
                        
                        # 3. Insert into property_photos or property_documents (only if NOT mail image)
                        if p_id and not is_mail_image: 
                            try:
                                if is_photo:
                                    # Check if photo record exists
                                    photo_check = self.supabase.table('property_photos')\
                                        .select('photo_id')\
                                        .eq('p_id', p_id)\
                                        .eq('file_path', relative_path)\
                                        .execute()
                                    
                                    if not photo_check.data:
                                        photo_record = {
                                            'p_id': p_id,
                                            'file_path': relative_path,
                                            'file_name': file,
                                            'cloud_path': cloud_path,
                                            'upload_date': datetime.now().isoformat(),
                                            'is_primary': False,
                                            'caption': ''
                                        }
                                        self.supabase.table('property_photos').insert(photo_record).execute()
                                        logger.debug(f"Linked photo to property {p_id}")
                                else:
                                    # Check if document record exists
                                    doc_check = self.supabase.table('property_documents')\
                                        .select('doc_id')\
                                        .eq('p_id', p_id)\
                                        .eq('file_path', relative_path)\
                                        .execute()
                                    
                                    if not doc_check.data:
                                        doc_record = {
                                            'p_id': p_id,
                                            'file_name': file,
                                            'file_path': relative_path,
                                            'cloud_path': cloud_path,
                                            'upload_date': datetime.now().isoformat(),
                                            'doc_type': os.path.splitext(file)[1].lower(),
                                            'description': ''
                                        }
                                        self.supabase.table('property_documents').insert(doc_record).execute()
                                        logger.debug(f"Linked document to property {p_id}")
                            except Exception as e:
                                logger.debug(f"Warning: Could not link file to property {p_id}: {e}")
                        
                        conn.commit()
                        stats['uploaded'] += 1
                        logger.debug(f"Uploaded and tracked: {file}")
                        
                    except Exception as e:
                        error_msg = f"Upload {relative_path}: {str(e)}"
                        stats['failed'].append(error_msg)
                        logger.debug(error_msg)
                        conn.rollback()
                        continue

    def _download_files_from_cloud(self, cursor, stats: dict, conn):
        """Download files from Supabase Storage based on Supabase DB records"""
        bucket = 'property-files'
        
        try:
            cursor.execute("SELECT MAX(last_sync_at) as max_sync FROM file_sync")
            result = cursor.fetchone()
            files_last_sync = result['max_sync'] if result and result['max_sync'] else '1970-01-01'
            
            logger.debug(f"Looking for files modified since: {files_last_sync}")
            
            cloud_files = self.supabase.table('file_sync')\
                .select('*')\
                .gt('modified_at', files_last_sync)\
                .execute()
            
            if not cloud_files.data:
                logger.debug("No new files found in Supabase since last sync")
                return
            
            logger.debug(f"Found {len(cloud_files.data)} new files to check")
            
            for file_record in cloud_files.data:
                cloud_path = file_record['cloud_path']
                relative_path = file_record['local_path']
                cloud_hash = file_record.get('file_hash')
                filename = os.path.basename(cloud_path)
                
                # CRITICAL FIX: Always normalize to forward slashes for consistency
                normalized_relative_path = relative_path.replace('\\', '/')
                
                # Build local path
                absolute_path = os.path.join(self.static_path, *normalized_relative_path.split('/'))
                absolute_path = os.path.normpath(absolute_path)

                logger.debug(f"BASE_DIR: {BASE_DIR}")
                logger.debug(f"static_path: {self.static_path}")
                logger.debug(f"Target download path: {absolute_path}")
                
                # Check if already have current version
                cursor.execute("""
                    SELECT file_hash FROM file_sync 
                    WHERE local_path = %s OR local_path = %s
                """, (relative_path, normalized_relative_path))  # Check both original and normalized

                
                existing = cursor.fetchone()
                if existing and existing['file_hash'] == cloud_hash:
                    logger.debug(f"  Already current, skipping")
                    continue
                
                # Download file
                try:
                    logger.debug(f"  Downloading from cloud...")
                    response = self.supabase.storage.from_(bucket).download(cloud_path)
                    
                    # Ensure directory exists
                    parent_dir = os.path.dirname(absolute_path)
                    if parent_dir:
                        os.makedirs(parent_dir, exist_ok=True)
                    
                    with open(absolute_path, 'wb') as f:
                        f.write(response)
                    
                    # Verify hash
                    local_hash = self._get_file_hash(absolute_path)
                    
                    if local_hash != cloud_hash:
                        logger.debug(f"  WARNING: Hash mismatch!")
                        stats['failed'].append(f"Hash mismatch: {filename}")
                        continue
                    
                    # CRITICAL FIX: Insert file_sync record FIRST (independent transaction)
                    try:
                        cursor.execute("""
                            INSERT INTO file_sync (local_path, cloud_path, file_hash, modified_at, sync_status, last_sync_at)
                            VALUES (%s, %s, %s, NOW(), 'synced', NOW())
                            ON CONFLICT (local_path) DO UPDATE SET
                            cloud_path = EXCLUDED.cloud_path,
                            file_hash = EXCLUDED.file_hash,
                            sync_status = 'synced',
                            last_sync_at = NOW()
                        """, (normalized_relative_path, cloud_path, local_hash))
                        conn.commit()  # Commit file tracking immediately!
                        stats['downloaded'] += 1
                        logger.debug(f"  File tracking saved")
                    except Exception as sync_error:
                        logger.debug(f"  ERROR saving file_sync: {sync_error}")
                        stats['failed'].append(f"Sync record failed: {filename}")
                        continue
                    
                    # CRITICAL FIX: Try to create property file records, but DON'T rollback file_sync on failure
                    p_id = self._extract_property_id_from_filename(filename)
                    if p_id:
                        try:
                            # Check if property exists before trying to link
                            cursor.execute("SELECT p_id FROM properties WHERE p_id = %s", (p_id,))
                            if not cursor.fetchone():
                                logger.debug(f"  Property {p_id} not yet synced, skipping DB link")
                                continue
                            
                            # Check if mail image using both local and cloud property metadata
                            local_mail_match = False
                            cursor.execute("""
                                SELECT p_mail_image_1, p_mail_image_2
                                FROM properties
                                WHERE p_id = %s
                            """, (p_id,))
                            prop_row = cursor.fetchone()

                            local_mail_paths = set()
                            if prop_row:
                                for key in ('p_mail_image_1', 'p_mail_image_2'):
                                    value = prop_row.get(key)
                                    if value:
                                        normalized = str(value).replace('\\', '/')
                                        local_mail_paths.add(normalized)
                                        local_mail_paths.add(os.path.basename(normalized))

                            cloud_mail_paths = self._get_cloud_mail_image_paths(p_id)
                            all_mail_paths = local_mail_paths | cloud_mail_paths

                            is_mail_image = (
                                filename in all_mail_paths
                                or normalized_relative_path in all_mail_paths
                            )
                            
                            if not is_mail_image:
                                if self._is_photo_file(filename):
                                    cursor.execute("""
                                        SELECT photo_id FROM property_photos 
                                        WHERE p_id = %s AND file_path = %s
                                    """, (p_id, normalized_relative_path))
                                    
                                    if not cursor.fetchone():
                                        cursor.execute("""
                                            INSERT INTO property_photos (p_id, file_path, file_name, upload_date, is_primary, caption)
                                            VALUES (%s, %s, %s, NOW(), FALSE, '')
                                        """, (p_id, normalized_relative_path, filename))
                                        logger.debug(f"  Created property_photos record")
                                else:
                                    cursor.execute("""
                                        SELECT doc_id FROM property_documents 
                                        WHERE p_id = %s AND file_path = %s
                                    """, (p_id, normalized_relative_path))
                                    
                                    if not cursor.fetchone():
                                        doc_type = os.path.splitext(filename)[1].lower()
                                        cursor.execute("""
                                            INSERT INTO property_documents (p_id, file_path, file_name, upload_date, doc_type, description)
                                            VALUES (%s, %s, %s, NOW(), %s, '')
                                        """, (p_id, normalized_relative_path, filename, doc_type))
                                        logger.debug(f"  Created property_documents record")
                                
                                conn.commit()  # Commit property link separately
                                
                        except Exception as db_error:
                            # Log but don't fail - file is saved, can relink later
                            logger.debug(f"  Warning: Could not link to property {p_id}: {db_error}")
                            # Don't rollback - file_sync is already committed
                    
                    logger.debug(f"  Successfully processed")
                    
                except Exception as e:
                    error_msg = f"Download {cloud_path}: {str(e)}"
                    stats['failed'].append(error_msg)
                    logger.debug(f"  ERROR: {error_msg}")
                    continue  # Don't rollback, just continue to next file
                    
            logger.debug("Skipping _reconcile_downloaded_files during normal sync")
                    
        except Exception as e:
            logger.debug(f"Error accessing cloud file list: {e}")
            stats['failed'].append(f"List error: {str(e)}")
            
    def _reconcile_downloaded_files(self, cursor, conn):
        """Ensure property_photos/property_documents records exist for downloaded files that still exist locally."""
        try:
            # Reconcile photos
            cursor.execute("""
                SELECT fs.local_path
                FROM file_sync fs
                LEFT JOIN property_photos pp ON fs.local_path = pp.file_path
                WHERE fs.local_path LIKE 'uploads/photos/%'
                  AND fs.sync_status = 'synced'
                  AND pp.photo_id IS NULL
            """)

            for row in cursor.fetchall():
                local_path = row['local_path']
                filename = os.path.basename(local_path)
                p_id = self._extract_property_id_from_filename(filename)

                absolute_path = os.path.normpath(os.path.join(self.static_path, *local_path.replace('\\', '/').split('/')))
                if not os.path.exists(absolute_path):
                    continue

                if p_id:
                    cursor.execute("SELECT p_id, p_mail_image_1, p_mail_image_2 FROM properties WHERE p_id = %s", (p_id,))
                    prop_row = cursor.fetchone()
                    if not prop_row:
                        continue

                    # Skip mail images
                    mail_paths = set()
                    for key in ('p_mail_image_1', 'p_mail_image_2'):
                        value = prop_row.get(key)
                        if value:
                            normalized = str(value).replace('\\', '/')
                            mail_paths.add(normalized)
                            mail_paths.add(os.path.basename(normalized))

                    if local_path in mail_paths or filename in mail_paths:
                        continue

                    cursor.execute("""
                        INSERT INTO property_photos (p_id, file_path, file_name, upload_date, is_primary, caption)
                        VALUES (%s, %s, %s, NOW(), FALSE, '')
                    """, (p_id, local_path, filename))

            # Reconcile documents
            cursor.execute("""
                SELECT fs.local_path
                FROM file_sync fs
                LEFT JOIN property_documents pd ON fs.local_path = pd.file_path
                WHERE fs.local_path LIKE 'uploads/documents/%'
                  AND fs.sync_status = 'synced'
                  AND pd.doc_id IS NULL
            """)

            for row in cursor.fetchall():
                local_path = row['local_path']
                filename = os.path.basename(local_path)
                p_id = self._extract_property_id_from_filename(filename)

                absolute_path = os.path.normpath(os.path.join(self.static_path, *local_path.replace('\\', '/').split('/')))
                if not os.path.exists(absolute_path):
                    continue

                if p_id:
                    cursor.execute("SELECT p_id FROM properties WHERE p_id = %s", (p_id,))
                    if not cursor.fetchone():
                        continue

                    doc_type = os.path.splitext(filename)[1].lower()
                    cursor.execute("""
                        INSERT INTO property_documents (p_id, file_path, file_name, upload_date, doc_type, description)
                        VALUES (%s, %s, %s, NOW(), %s, '')
                    """, (p_id, local_path, filename, doc_type))

            conn.commit()
            logger.info("Reconciled missing photo/document records from downloaded files")

        except Exception as e:
            logger.error(f"Error reconciling downloaded files: {e}")
            
    def _extract_property_id_from_filename(self, filename: str) -> Optional[int]:
        parts = filename.split('_')
        if parts and parts[0].isdigit():
            return int(parts[0])
        return None

    def _is_photo_file(self, filename: str) -> bool:
        photo_exts = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'}
        return os.path.splitext(filename)[1].lower() in photo_exts
    
    def _get_file_hash(self, filepath: str) -> str:
        """Calculate SHA256 hash of file"""
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def sync_start_session(self) -> dict:
        return self.sync_database('from_cloud')

    def sync_end_session(self) -> dict:
        return self.sync_database('to_cloud')
    
    def get_sync_status(self) -> dict:
        """Get current sync status summary"""
        conn = self.get_local_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE sync_status = 'pending') as pending,
                    COUNT(*) FILTER (WHERE sync_status = 'synced') as synced,
                    COUNT(*) FILTER (WHERE sync_status = 'conflict') as conflicts
                FROM properties
            """)
            prop_stats = cursor.fetchone()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE sync_status = 'pending') as pending,
                    COUNT(*) FILTER (WHERE sync_status = 'synced') as synced
                FROM owners
            """)
            owner_stats = cursor.fetchone()
            
            cursor.execute("""
                SELECT COUNT(*) FROM file_sync WHERE sync_status = 'pending'
            """)
            pending_files = cursor.fetchone()[0]
            
            return {
                'properties_pending': prop_stats[0],
                'properties_synced': prop_stats[1],
                'properties_conflicts': prop_stats[2],
                'owners_pending': owner_stats[0],
                'owners_synced': owner_stats[1],
                'files_pending': pending_files,
                'last_sync': self._get_last_sync_time(cursor)
            }
        finally:
            cursor.close()
            conn.close()
    
    def _get_last_sync_time(self, cursor):
        cursor.execute("SELECT MAX(created_at) FROM sync_log")
        result = cursor.fetchone()
        return result[0].isoformat() if result[0] else None