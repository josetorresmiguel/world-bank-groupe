-- models/staging/stg_indicateurs.sql

with source as (

    select * from {{ source('world_bank', 'raw_data') }}

),

derniere_version as (

    -- Le raw est append-only : une même ligne pays-année peut exister
    -- plusieurs fois (valeurs corrigées par l'API). On garde la plus récente.
    select *
    from source
    qualify row_number() over (
        partition by countryiso3code, annee
        order by inserted_at desc
    ) = 1

),

cleaned as (

    select
        countryiso3code as pays_code,
        country_name    as pays_nom,
        annee,

        PIB,
        PIB_par_habitant,
        Croissance_PIB,
        Population,
        Esperance_vie,
        Chomage,
        Inflation,
        Acces_electricite,
        Taux_natalite,
        Scolarisation_secondaire,
        Scolarisation_superieur,
        Achevement_primaire,
        Depense_de_sante,
        Depense_publique_education,
        Alphabetisation_adultes

    from derniere_version
    where countryiso3code is not null
      and countryiso3code != ''
      and annee is not null

)

select * from cleaned