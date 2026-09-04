-- models/staging/stg_indicateurs.sql

with source as (
    select * from {{ source('world_bank', 'raw_data') }}
),

derniere_version as (
    -- Le raw est append-only : une même observation peut exister plusieurs
    -- fois. On garde la plus récente.
    select *
    from source
    qualify row_number() over (
        partition by countryiso3code, `date`, indicator.id
        order by inserted_at desc
    ) = 1
),

cleaned as (
    select
        v.countryiso3code           as pays_code,
        v.country.value             as pays_nom,
        cast(v.`date` as int64)     as annee,
        v.indicator.id              as indicateur_code,
        v.indicator.value           as indicateur_nom,
        cast(v.value as float64)    as valeur
    from derniere_version as v
    where v.countryiso3code is not null
      and v.countryiso3code != ''
      and v.`date` is not null
)

select * from cleaned