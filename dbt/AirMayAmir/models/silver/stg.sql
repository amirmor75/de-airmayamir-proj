{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='example_id'
) }}

-- empty silver model
-- add your SELECT ... FROM source here

{% if is_incremental() %}
-- optional filtering
-- WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
