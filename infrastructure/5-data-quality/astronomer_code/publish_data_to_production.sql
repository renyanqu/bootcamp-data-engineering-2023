 INSERT INTO {{var.value.SCHEMA}}.{{var.value.PROD_TABLE}}
    SELECT * FROM {{var.value.SCHEMA}}.{{var.value.STAGING_TABLE}}
    WHERE date = DATE('{{var.value.RUN_DATE}}');
    
    DELETE FROM {{var.value.SCHEMA}}.{{var.value.STAGING_TABLE}}
    WHERE date = DATE('{{var.value.RUN_DATE}}')