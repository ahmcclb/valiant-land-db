#!/usr/bin/env python3
"""
Valiant Land Database Cleanup Script
Clears all owner and property records for fresh start with real data
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import sys

# Database configuration - Update these with your connection details
DB_CONFIG = {
    'dbname': 'valiant_land',      # Change to your database name
    'user': 'postgres',            # Change to your username
    'password': 'zPF3R9xYPz0sO6bp',   # Change to your password
    'host': 'localhost',           # Change if remote host
    'port': '5432'                 # Change if different port
}

# Tables to truncate (in order respecting foreign key constraints)
TABLES_TO_TRUNCATE = [
    'property_tags',        # Junction table first
    'property_photos',      # Property media
    'property_documents',   # Property documents
    'property_links',       # Property links
    'properties',           # Main property records
    'owners'                # Owner records last
]

# Tables to preserve (lookup/config data)
TABLES_TO_PRESERVE = [
    'statuses',
    'tags',
    'companies',
    'document_templates',
    'generated_documents'
]

def get_connection():
    """Establish database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"✓ Connected to database: {DB_CONFIG['dbname']}")
        return conn
    except psycopg2.Error as e:
        print(f"✗ Connection failed: {e}")
        sys.exit(1)

def get_counts(conn):
    """Get current record counts for display"""
    cursor = conn.cursor()
    counts = {}
    
    all_tables = TABLES_TO_TRUNCATE + TABLES_TO_PRESERVE
    
    for table in all_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cursor.fetchone()[0]
        except psycopg2.Error:
            counts[table] = None
    
    cursor.close()
    return counts

def confirm_deletion(counts):
    """Show user what will be deleted and ask for confirmation"""
    print("\n" + "="*60)
    print("DATABASE CLEANUP PREVIEW")
    print("="*60)
    
    print("\n📋 RECORDS TO BE DELETED:")
    print("-" * 40)
    total_records = 0
    
    for table in TABLES_TO_TRUNCATE:
        count = counts.get(table, 0) or 0
        total_records += count
        status = f"({count:,} records)" if count is not None else "(table not found)"
        print(f"  • {table:<25} {status}")
    
    print("-" * 40)
    print(f"  TOTAL TO DELETE: {total_records:,} records")
    
    print("\n🔒 TABLES PRESERVED (LOOKUP DATA):")
    print("-" * 40)
    for table in TABLES_TO_PRESERVE:
        count = counts.get(table, 0) or 0
        status = f"({count:,} records)" if count is not None else "(table not found)"
        print(f"  • {table:<25} {status}")
    
    print("\n" + "⚠️  WARNING: This action cannot be undone!")
    print("="*60)
    
    # Double confirmation for safety
    confirm1 = input("\nType 'DELETE' to proceed with deletion: ")
    if confirm1.strip().upper() != 'DELETE':
        print("\n✗ Operation cancelled by user")
        return False
    
    confirm2 = input("Are you absolutely sure? Type 'YES' to confirm: ")
    if confirm2.strip().upper() != 'YES':
        print("\n✗ Operation cancelled by user")
        return False
    
    return True

def truncate_tables(conn):
    """Truncate tables in transaction"""
    cursor = conn.cursor()
    deleted_counts = {}
    
    try:
        # Disable foreign key checks temporarily (PostgreSQL specific)
        cursor.execute("SET session_replication_role = replica;")
        
        for table in TABLES_TO_TRUNCATE:
            try:
                # Get count before deletion
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                before_count = cursor.fetchone()[0]
                
                # Truncate table (faster than DELETE, resets sequences)
                cursor.execute(f"TRUNCATE TABLE {table} CASCADE")
                deleted_counts[table] = before_count
                
                print(f"✓ Truncated {table:<25} ({before_count:,} records removed)")
                
            except psycopg2.Error as e:
                print(f"✗ Error truncating {table}: {e}")
                deleted_counts[table] = f"ERROR: {e}"
        
        # Re-enable foreign key checks
        cursor.execute("SET session_replication_role = DEFAULT;")
        
        conn.commit()
        return deleted_counts
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Transaction rolled back due to error: {e}")
        raise
    finally:
        cursor.close()

def reset_sequences(conn):
    """Reset sequences for truncated tables"""
    cursor = conn.cursor()
    sequences = [
        ('properties', 'p_id'),
        ('owners', 'or_id'),
        ('property_tags', 'pt_id'),
        ('property_photos', 'photo_id'),
        ('property_documents', 'doc_id'),
        ('property_links', 'link_id')
    ]
    
    print("\n🔄 Resetting sequences...")
    
    for table, column in sequences:
        try:
            # PostgreSQL sequence reset
            cursor.execute(f"""
                SELECT setval(pg_get_serial_sequence('{table}', '{column}'), 1, false)
                WHERE EXISTS (
                    SELECT 1 FROM pg_sequences 
                    WHERE schemaname = 'public' 
                    AND sequencename = '{table}_{column}_seq'
                )
            """)
            print(f"  ✓ Reset sequence for {table}.{column}")
        except psycopg2.Error:
            pass  # Sequence might not exist
    
    conn.commit()
    cursor.close()

def verify_cleanup(conn):
    """Verify tables are empty"""
    cursor = conn.cursor()
    print("\n✅ VERIFICATION - Tables should show 0 records:")
    print("-" * 40)
    
    all_empty = True
    for table in TABLES_TO_TRUNCATE:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        status = "✓ EMPTY" if count == 0 else f"✗ Still has {count} records!"
        print(f"  {table:<25} {status}")
        if count > 0:
            all_empty = False
    
    cursor.close()
    return all_empty

def backup_option():
    """Offer to create backup before deletion"""
    print("\n💾 BACKUP RECOMMENDATION:")
    print("Before clearing data, consider creating a backup with:")
    print(f"  pg_dump -U {DB_CONFIG['user']} -d {DB_CONFIG['dbname']} > backup_before_cleanup.sql")
    
    backup = input("\nHave you created a backup? (yes/no): ")
    return backup.strip().lower() in ['yes', 'y']

def main():
    print("Valiant Land Database Cleanup Tool")
    print("=================================")
    print("This script will remove all owner and property records")
    print("while preserving statuses, tags, and company settings.\n")
    
    # Check if user has backup
    if not backup_option():
        print("\n⚠️  Please create a backup first, then run this script again.")
        sys.exit(0)
    
    # Connect to database
    conn = get_connection()
    
    try:
        # Get current counts
        counts = get_counts(conn)
        
        # Confirm with user
        if not confirm_deletion(counts):
            sys.exit(0)
        
        # Perform truncation
        print("\n🗑️  DELETING RECORDS...")
        print("-" * 40)
        deleted = truncate_tables(conn)
        
        # Reset sequences
        reset_sequences(conn)
        
        # Verify
        success = verify_cleanup(conn)
        
        # Summary
        print("\n" + "="*60)
        print("CLEANUP SUMMARY")
        print("="*60)
        total_deleted = sum(v for v in deleted.values() if isinstance(v, int))
        print(f"Total records removed: {total_deleted:,}")
        print(f"Tables truncated: {len(TABLES_TO_TRUNCATE)}")
        print(f"Sequences reset: Yes")
        
        if success:
            print("\n✅ Database is clean and ready for real data!")
            print("You can now begin importing your production data.")
        else:
            print("\n⚠️  Some tables may still contain data. Check errors above.")
            
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        sys.exit(1)
    finally:
        conn.close()
        print(f"\n✓ Database connection closed")

if __name__ == "__main__":
    main()