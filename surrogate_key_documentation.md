# Surrogate Key Solution for Snowflake-PostgreSQL Sync

## Problem Statement

When syncing data from Snowflake to PostgreSQL for Salesforce integration, some tables lack single primary keys or have composite primary keys. Salesforce requires unique identifiers for each record to properly handle updates and deletes. Without proper primary keys, it becomes difficult to:

1. Track record changes (updates vs deletes)
2. Maintain data integrity in Salesforce
3. Identify which records have been modified or removed

## Solution Overview

The enhanced sync script introduces **surrogate key generation** to create unique identifiers for records that lack proper primary keys. This solution provides two methods:

### 1. Hash-Based Surrogate Keys (Recommended)
- **Deterministic**: Same input always produces the same key
- **Consistent**: Keys remain stable across sync runs
- **Efficient**: Generated using MD5 hash of key columns
- **Use Case**: Tables with composite keys or unique constraints

### 2. Sequence-Based Surrogate Keys
- **Auto-incrementing**: Uses PostgreSQL sequences
- **Unique**: Guarantees uniqueness across all records
- **Simple**: Easy to implement and understand
- **Use Case**: Tables with no natural keys or when hash collisions are a concern

## Configuration

Add a `surrogate_key` section to your table configuration:

```json
{
  "entity_name": "your_table",
  "surrogate_key": {
    "enabled": true,
    "method": "hash",
    "source_columns": ["col1", "col2", "col3"],
    "column_name": "salesforce_id",
    "column_type": "VARCHAR(32)",
    "use_for_conflict": true,
    "description": "Hash-based surrogate key for Salesforce integration"
  }
}
```

### Configuration Options

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `enabled` | boolean | Enable surrogate key generation | `false` |
| `method` | string | Generation method: "hash" or "sequence" | `"hash"` |
| `source_columns` | array | Columns to use for key generation | `key_columns` |
| `column_name` | string | Name of surrogate key column | `"surrogate_key"` |
| `column_type` | string | PostgreSQL column type | `"VARCHAR(32)"` |
| `use_for_conflict` | boolean | Use surrogate key for conflict resolution | `true` |
| `sequence_name` | string | Sequence name (for sequence method) | `"{entity_name}_surrogate_seq"` |
| `description` | string | Documentation for the surrogate key | `""` |

## Implementation Details

### Hash-Based Keys
```sql
-- Generated in Snowflake query
MD5(CONCAT(col1, '|', col2, '|', col3)) AS salesforce_id

-- Or in Python
import hashlib
key = hashlib.md5('|'.join([str(val) for val in values]).encode()).hexdigest()
```

### Sequence-Based Keys
```sql
-- Create sequence
CREATE SEQUENCE IF NOT EXISTS table_surrogate_seq START WITH 1;

-- Use in upsert
COALESCE(existing_key, nextval('table_surrogate_seq'))
```

## Database Schema Changes

The script automatically:
1. Creates surrogate key columns in target tables
2. Creates sequences for sequence-based keys
3. Updates staging tables to include surrogate keys
4. Modifies conflict resolution to use surrogate keys

## Change Tracking Enhancement

With surrogate keys, the system can now:

### Track Updates
- Compare row hashes to detect changes
- Update `updated_on` timestamp
- Maintain `last_seen_at` for activity tracking

### Track Deletes
- Mark records as inactive (`is_active = FALSE`)
- Set `deleted_at` timestamp
- Preserve historical data for audit trails

### Salesforce Integration
- Use surrogate key as External ID in Salesforce
- Enable proper upsert operations
- Maintain referential integrity

## Usage Examples

### Example 1: Composite Primary Key Table
```json
{
  "entity_name": "case_agent_mapping",
  "key_columns": ["case_id", "agent_id", "role_type"],
  "surrogate_key": {
    "enabled": true,
    "method": "hash",
    "source_columns": ["case_id", "agent_id", "role_type"],
    "column_name": "mapping_key",
    "use_for_conflict": true
  }
}
```

### Example 2: No Natural Primary Key
```json
{
  "entity_name": "activity_log",
  "key_columns": ["timestamp", "user_id", "action"],
  "surrogate_key": {
    "enabled": true,
    "method": "sequence",
    "column_name": "log_id",
    "column_type": "BIGINT",
    "use_for_conflict": true
  }
}
```

### Example 3: Additional Identifier for Salesforce
```json
{
  "entity_name": "customer_profile",
  "key_columns": ["email"],
  "surrogate_key": {
    "enabled": true,
    "method": "hash",
    "source_columns": ["email", "first_name", "last_name"],
    "column_name": "customer_key",
    "use_for_conflict": false
  }
}
```

## Migration Guide

### Step 1: Backup Current Data
```sql
-- Create backup tables
CREATE TABLE backup_table_name AS SELECT * FROM original_table;
```

### Step 2: Update Configuration
Add surrogate key configuration to affected tables in `tables_config.json`.

### Step 3: Run Enhanced Sync
```bash
python enhanced_sync_with_surrogate_keys.py
```

### Step 4: Verify Results
```sql
-- Check surrogate key generation
SELECT 
    original_key_columns,
    surrogate_key_column,
    COUNT(*) as record_count
FROM target_table 
GROUP BY original_key_columns, surrogate_key_column
HAVING COUNT(*) > 1; -- Should return no rows
```

### Step 5: Update Salesforce Integration
- Configure External ID field in Salesforce
- Update integration mappings to use surrogate keys
- Test upsert operations

## Best Practices

### 1. Choose the Right Method
- **Hash**: For tables with stable composite keys
- **Sequence**: For tables with no natural keys or frequent schema changes

### 2. Key Column Selection
- Include all columns that make a record unique
- Avoid frequently changing columns in hash keys
- Consider business logic requirements

### 3. Conflict Resolution
- Set `use_for_conflict: true` for primary identification
- Set `use_for_conflict: false` for additional identifiers

### 4. Monitoring
- Monitor for hash collisions (very rare with MD5)
- Track sequence usage and performance
- Validate data integrity after migration

### 5. Documentation
- Document surrogate key purpose and generation logic
- Maintain mapping between natural and surrogate keys
- Update data dictionaries and schemas

## Troubleshooting

### Common Issues

#### Hash Collisions
```sql
-- Check for hash collisions
SELECT surrogate_key, COUNT(*) 
FROM table_name 
GROUP BY surrogate_key 
HAVING COUNT(*) > 1;
```

#### Missing Surrogate Keys
```sql
-- Find records without surrogate keys
SELECT * FROM table_name WHERE surrogate_key IS NULL;
```

#### Sequence Issues
```sql
-- Reset sequence if needed
SELECT setval('sequence_name', (SELECT MAX(surrogate_key) FROM table_name));
```

### Performance Considerations
- Hash generation adds minimal overhead
- Sequence-based keys may cause contention under high load
- Index surrogate key columns for optimal performance
- Consider partitioning for very large tables

## Limitations

1. **Hash Method**: Theoretical possibility of collisions (extremely rare)
2. **Sequence Method**: Requires careful sequence management
3. **Storage**: Additional column storage overhead
4. **Complexity**: Increased configuration complexity

## Future Enhancements

1. **UUID Support**: Add UUID-based surrogate keys
2. **Custom Algorithms**: Support for custom key generation
3. **Batch Optimization**: Optimize for very large datasets
4. **Monitoring Dashboard**: Real-time key generation monitoring
5. **Automatic Detection**: Auto-detect tables needing surrogate keys