-- models/marts/dim_pays.sql
-- Une ligne par pays.
-- region et income_level ne sont pas disponibles dans l'API indicateurs :
-- ils viendront de l'endpoint /country dans une prochaine itération.

{{ config(materialized='table') }}

with pays as (

    select
        pays_code,
        any_value(pays_nom) as pays_nom
    from {{ ref('stg_indicateurs') }}
    where pays_code is not null
    group by pays_code

)

select
    to_hex(md5(pays_code))  as pays_id,
    pays_code               as countryiso3code,
    pays_nom                as country_name,
    cast(null as string)    as region,
    cast(null as string)    as income_level
from pays