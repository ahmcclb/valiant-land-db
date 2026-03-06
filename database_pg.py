import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
import os
from datetime import datetime
from contextlib import contextmanager

# Database configuration - UPDATE THESE VALUES
DB_CONFIG = {
    'host': 'localhost',
    'database': 'valiant_land',
    'user': 'vl_user',
    'password': 'ifoLdouTEAThleCt',  # Change this!
    'port': '5432'
}

def get_db_connection():
    """Create a database connection."""
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

@contextmanager
def get_db_cursor(commit=False):
    """Context manager for database operations."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield cursor
        if commit:
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def init_database():
    """Initialize the PostgreSQL database with all tables."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("Initializing PostgreSQL Database...")
    
    # 1. STATUSES TABLE
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS statuses (
            status_id SERIAL PRIMARY KEY,
            s_status TEXT UNIQUE NOT NULL,
            s_color TEXT DEFAULT '#808080',
            s_order INTEGER DEFAULT 0,
            s_is_active BOOLEAN DEFAULT TRUE
        )
    ''')
    
    # 2. TAGS TABLE  
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tags (
            tag_id SERIAL PRIMARY KEY,
            tag_name TEXT UNIQUE NOT NULL,
            tag_color TEXT DEFAULT '#808080',
            tag_description TEXT
        )
    ''')
    
    # 3. OWNERS TABLE
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS owners (
            or_id SERIAL PRIMARY KEY,
            o_type TEXT CHECK(o_type IN ('Company', 'Individual')),
            or_fname TEXT,
            or_lname TEXT,
            or_email TEXT,
            or_phone TEXT,
            or_fax TEXT,
            o_fname TEXT,
            o_lname TEXT,
            o_2fname TEXT,
            o_2lname TEXT,
            o_3fname TEXT,
            o_3lname TEXT,
            o_4fname TEXT,
            o_4lname TEXT,
            o_5fname TEXT,
            o_5lname TEXT,
            o_company TEXT,
            o_multiple BOOLEAN DEFAULT FALSE,
            o_other_owners BOOLEAN DEFAULT FALSE,
            or_m_address TEXT,
            or_m_address2 TEXT,
            or_m_city TEXT,
            or_m_state TEXT,
            or_m_zip TEXT
        )
    ''')
    
    # 4. PROPERTIES TABLE
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS properties (
            p_id SERIAL PRIMARY KEY,
            or_id INTEGER NOT NULL REFERENCES owners(or_id),
            p_status_id INTEGER REFERENCES statuses(status_id),
            p_state TEXT,
            p_longstate TEXT,
            p_county TEXT,
            p_address TEXT,
            p_city TEXT,
            p_zip TEXT,
            p_apn TEXT,
            p_acres REAL,
            p_sqft INTEGER,
            p_terrain TEXT,
            p_short_legal TEXT,
            p_zoning TEXT,
            p_use TEXT,
            p_use_code TEXT,
            p_use_description TEXT,
            p_restrictions TEXT,
            p_flood TEXT,
            p_flood_description TEXT,
            p_environmental TEXT,
            p_price REAL,
            p_liens REAL,
            p_back_tax REAL,
            p_base_tax REAL,
            p_comp_market_value REAL,
            p_county_market_value REAL,
            p_county_assessed_value REAL,
            p_sale_price REAL,
            p_hoa REAL,
            p_impact_fee REAL,
            p_min_acceptable_offer REAL,
            p_max_offer_amount REAL,
            p_est_value REAL,
            p_improvements TEXT,
            p_power TEXT,
            p_access TEXT,
            p_waste_system_requirement TEXT,
            p_water_system_requirement TEXT,
            p_survey BOOLEAN DEFAULT FALSE,
            p_owned TEXT,
            p_aquired TEXT,
            p_listed BOOLEAN DEFAULT FALSE,
            p_agent_name TEXT,
            p_agent_phone TEXT,
            p_viable BOOLEAN DEFAULT FALSE,
            p_m_date TEXT,
            p_offer_accept_date TEXT,
            p_contract_expires_date TEXT,
            p_purchased_on TEXT,
            p_purchase_amount REAL,
            p_purchase_closing_costs REAL,
            p_closing_company_name_purchase TEXT,
            p_sold_on TEXT,
            p_buyer TEXT,
            p_sold_amount REAL,
            p_sold_closing_costs REAL,
            p_profit REAL,
            p_closing_company_name_sale TEXT,
            p_plat_map_link TEXT,
            p_comments TEXT,
            p_note TEXT,
            p_betty_score INTEGER,
            p_create_time TEXT,
            p_last_updated TEXT,
            p_status_last_updated TEXT,
            p_last_sold_date TEXT,
            p_last_sold_amount REAL,
            p_last_transaction_date TEXT,
            p_last_transaction_doc_type TEXT
        )
    ''')
    
    # Add mail image columns to properties table
    cursor.execute('''
        ALTER TABLE properties 
        ADD COLUMN IF NOT EXISTS p_mail_image_1 TEXT
    ''')
    cursor.execute('''
        ALTER TABLE properties 
        ADD COLUMN IF NOT EXISTS p_mail_image_2 TEXT
    ''')
    
    # 5. PROPERTY-TAG JUNCTION TABLE
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS property_tags (
            p_id INTEGER REFERENCES properties(p_id) ON DELETE CASCADE,
            tag_id INTEGER REFERENCES tags(tag_id) ON DELETE CASCADE,
            PRIMARY KEY (p_id, tag_id)
        )
    ''')
    
    # 6. PROPERTY PHOTOS - WITH SYNC COLUMNS
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS property_photos (
            photo_id SERIAL PRIMARY KEY,
            p_id INTEGER NOT NULL REFERENCES properties(p_id) ON DELETE CASCADE,
            file_path TEXT NOT NULL,
            file_name TEXT,
            upload_date TEXT,
            caption TEXT,
            is_primary BOOLEAN DEFAULT FALSE,
            cloud_path TEXT,
            modified_at TIMESTAMP DEFAULT NOW(),
            last_sync_at TIMESTAMP,
            sync_status TEXT DEFAULT 'synced',
            sync_version INTEGER DEFAULT 1
        )
    ''')
    
    # 7. PROPERTY DOCUMENTS - WITH SYNC COLUMNS
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS property_documents (
            doc_id SERIAL PRIMARY KEY,
            p_id INTEGER NOT NULL REFERENCES properties(p_id) ON DELETE CASCADE,
            file_path TEXT NOT NULL,
            file_name TEXT,
            doc_type TEXT,
            upload_date TEXT,
            description TEXT,
            cloud_path TEXT,
            modified_at TIMESTAMP DEFAULT NOW(),
            last_sync_at TIMESTAMP,
            sync_status TEXT DEFAULT 'synced',
            sync_version INTEGER DEFAULT 1
        )
    ''')
    
    # 8. PROPERTY LINKS - WITH SYNC COLUMNS
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS property_links (
            link_id SERIAL PRIMARY KEY,
            p_id INTEGER NOT NULL REFERENCES properties(p_id) ON DELETE CASCADE,
            url TEXT NOT NULL,
            description TEXT,
            added_date TEXT,
            modified_at TIMESTAMP DEFAULT NOW(),
            last_sync_at TIMESTAMP,
            sync_status TEXT DEFAULT 'synced',
            sync_version INTEGER DEFAULT 1
        )
    ''')

    
    # 9. COMPANY INFORMATION
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            c_id SERIAL PRIMARY KEY,
            c_name TEXT,
            c_phone TEXT,
            c_fax TEXT,
            c_email TEXT,
            c_address TEXT,
            c_city TEXT,
            c_state TEXT,
            c_zip TEXT,
            c_nphone TEXT,
            c_ophone TEXT,
            c_sig_path TEXT,
            c_url TEXT
        )
    ''')
    
    # 10. DOCUMENT TEMPLATES
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS document_templates (
            template_id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            filename TEXT NOT NULL,
            merge_fields TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 11. GENERATED DOCUMENTS
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS generated_documents (
            doc_id SERIAL PRIMARY KEY,
            p_ids TEXT NOT NULL,
            template_id INTEGER REFERENCES document_templates(template_id),
            file_path TEXT NOT NULL,
            generated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_temp BOOLEAN DEFAULT FALSE
        )
    ''')
    
    # 12. SYNC LOG
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sync_log (
            log_id SERIAL PRIMARY KEY,
            table_name VARCHAR(50) NOT NULL,
            record_id INTEGER NOT NULL,
            operation VARCHAR(20) NOT NULL,
            direction VARCHAR(20),
            status VARCHAR(20),
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')        
    
    # 13. Add sync tracking columns to properties table
    cursor.execute('''
        ALTER TABLE properties 
        ADD COLUMN IF NOT EXISTS sync_status VARCHAR(20) DEFAULT 'pending'
    ''')
    cursor.execute('''
        ALTER TABLE properties 
        ADD COLUMN IF NOT EXISTS modified_at TIMESTAMP DEFAULT NOW()
    ''')
    cursor.execute('''
        ALTER TABLE properties 
        ADD COLUMN IF NOT EXISTS last_sync_at TIMESTAMP
    ''')
    cursor.execute('''
        ALTER TABLE properties 
        ADD COLUMN IF NOT EXISTS sync_version INTEGER DEFAULT 1
    ''')
    
    # 14. Add sync tracking columns to owners table
    cursor.execute('''
        ALTER TABLE owners 
        ADD COLUMN IF NOT EXISTS sync_status VARCHAR(20) DEFAULT 'pending'
    ''')
    cursor.execute('''
        ALTER TABLE owners 
        ADD COLUMN IF NOT EXISTS modified_at TIMESTAMP DEFAULT NOW()
    ''')
    cursor.execute('''
        ALTER TABLE owners 
        ADD COLUMN IF NOT EXISTS last_sync_at TIMESTAMP
    ''')
    cursor.execute('''
        ALTER TABLE owners 
        ADD COLUMN IF NOT EXISTS sync_version INTEGER DEFAULT 1
    ''')
    cursor.execute('''
        ALTER TABLE owners 
        ADD COLUMN IF NOT EXISTS sync_source VARCHAR(20)
    ''')

    # 15. FILE SYNC TABLE
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_sync (
            local_path TEXT PRIMARY KEY,
            cloud_path TEXT,
            file_hash TEXT,
            modified_at TIMESTAMP DEFAULT NOW(),
            last_sync_at TIMESTAMP,
            sync_status VARCHAR(20) DEFAULT 'pending'
        )
    ''')    

    # 16. SYNC DELETIONS TABLE (Tombstones for hard deletes)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sync_deletions (
            deletion_id SERIAL PRIMARY KEY,
            table_name VARCHAR(50) NOT NULL,
            record_id INTEGER NOT NULL,
            deleted_at TIMESTAMP DEFAULT NOW(),
            sync_status VARCHAR(20) DEFAULT 'pending',
            cloud_deleted BOOLEAN DEFAULT FALSE
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_deletions_status ON sync_deletions(sync_status, table_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_deletions_table_record ON sync_deletions(table_name, record_id)')
    
    # Create indexes for performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_property_or_id ON properties(or_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_property_status ON properties(p_status_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_property_county ON properties(p_county)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_property_state ON properties(p_state)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_owner_name ON owners(or_lname, or_fname)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_owner_company ON owners(o_company)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_owners_sync ON owners(sync_status)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_owners_modified ON owners(modified_at, last_sync_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_properties_sync ON properties(sync_status)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_properties_modified ON properties(modified_at, last_sync_at)')
    
    conn.commit()
    conn.close()
    print("✓ PostgreSQL schema created successfully!")

def insert_default_statuses():
    """Insert all 26 default status values."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    default_statuses = [
        ('Prospect', '#17a2b8', 1),
        ('Mailed Letter 1', '#ffc107', 2),
        ('Pending Preliminary Research', '#6c757d', 3),
        ('Ready to Email Offer', '#007bff', 4),
        ('Ready to Send Offer', '#007bff', 5),
        ('Offers Sent', '#28a745', 6),
        ('Offers Emailed', '#28a745', 7),
        ('Blind Offers Sent', '#28a745', 8),
        ('Ready to Send 2nd Offer', '#fd7e14', 9),
        ('2nd Offer Sent', '#28a745', 10),
        ('Pending 2nd Offer Research', '#6c757d', 11),
        ('End of Year 2nd Offer Sent', '#28a745', 12),
        ('Ready to Resend Blind Offer4', '#fd7e14', 13),
        ('Open Escrow - Detailed Research', '#6610f2', 14),
        ('Complete/ Ready To Sell', '#20c997', 15),
        ('Found Buyer - Open Escrow', '#6610f2', 16),
        ('Doesn\'t want to sell - DRIP', '#dc3545', 17),
        ('SKIP TRACE', '#6c757d', 18),
        ('ON HOLD', '#6c757d', 19),
        ('FILE CLOSED', '#6c757d', 20),
        ('SOLD', '#198754', 21),
        ('UNASSIGNED', '#6c757d', 22),
        ('RV Prospect', '#17a2b8', 23),
        ('Texting Initiated', '#ffc107', 24),
        ('Options Sent', '#007bff', 25),
        ('Hold Prospect', '#17a2b8', 26)
    ]
    
    for status, color, order in default_statuses:
        cursor.execute('''
            INSERT INTO statuses (s_status, s_color, s_order) 
            VALUES (%s, %s, %s)
            ON CONFLICT (s_status) DO NOTHING
        ''', (status, color, order))
    
    conn.commit()
    conn.close()
    print("✓ All 26 default statuses inserted")

if __name__ == '__main__':
    print("Initializing PostgreSQL Database...")
    print("-" * 40)
    init_database()
    insert_default_statuses()
    print("-" * 40)
    print("Database is ready to use!")
    
def close_all_connections():
    """Close all database connections - called on app shutdown"""
    global pool
    try:
        if 'pool' in globals() and pool:
            pool.closeall()
            print("Database connections closed")
    except Exception as e:
        print(f"Error closing database connections: {e}")