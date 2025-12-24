{% macro copy_into_inventory() %}
    COPY INTO supply_chain_db.landing.raw_inventory
    FROM (
                SELECT
            $1 ::STRING  AS item_id,
            $2 ::STRING  AS item_name,
            $3 ::STRING  AS category,
            $4 ::STRING  AS variant_name,
            $5 ::STRING  AS fuel_type,
            METADATA$FILENAME,
            METADATA$FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED,
            CURRENT_TIMESTAMP  -- as load_ts
        FROM @Supply_EXT_STAGE_STORAGE/raw_item_catalog
            (FILE_FORMAT => 'SUPPLY_CHAIN_db.LANDING.MY_CSV_FORMAT'))
             ON_ERROR = 'CONTINUE';
      
{% endmacro %}
