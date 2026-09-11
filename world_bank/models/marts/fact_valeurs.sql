-- models/marts/fact_valeurs.sql
-- Une ligne par pays-année-indicateur.
-- Les clés sont recalculées avec la même formule que dans les dimensions.

{{ config(materialized='table') }}

select
    to_hex(md5(pays_code))              as pays_id,
    to_hex(md5(cast(annee as string)))  as annee_id,
    to_hex(md5(indicateur_code))        as indicateur_id,
    valeur
from {{ ref('stg_indicateurs') }}
where valeur is not null