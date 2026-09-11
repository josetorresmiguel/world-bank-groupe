-- models/marts/dim_annee.sql
-- Une ligne par année présente dans les données.

{{ config(materialized='table') }}

with annees as (

    select distinct
        annee
    from {{ ref('stg_indicateurs') }}
    where annee is not null

)

select
    to_hex(md5(cast(annee as string)))  as annee_id,
    annee
from annees