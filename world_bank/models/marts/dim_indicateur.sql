-- models/marts/dim_indicateur.sql
-- Une ligne par indicateur, enrichie avec la catégorie du seed.

{{ config(materialized='table') }}

with indicateurs as (

    select
        indicateur_code,
        any_value(indicateur_nom) as indicateur_nom
    from {{ ref('stg_indicateurs') }}
    where indicateur_code is not null
    group by indicateur_code

)

select
    to_hex(md5(i.indicateur_code))       as indicateur_id,
    i.indicateur_code                    as indicator_code,
    i.indicateur_nom                     as indicator_name,
    coalesce(c.categorie, 'non_classe')  as categorie
from indicateurs as i
left join {{ ref('categories_indicateurs') }} as c
    on i.indicateur_code = c.indicator_code