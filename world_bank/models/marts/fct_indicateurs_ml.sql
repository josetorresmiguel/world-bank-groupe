-- models/marts/fct_indicateurs_ml.sql
-- Une ligne par pays-année, avec des features calculées pour le ML.

with source as (

    select * from {{ ref('int_indicateurs') }}
    where annee between 2000 and 2023

),

features as (

    select
        pays_code,
        pays_nom,
        annee,

        -- Indicateurs bruts
        PIB,
        PIB_par_habitant,
        Population,
        Esperance_vie,
        Chomage,
        Inflation,
        Acces_electricite,
        Alphabetisation_adultes,

        -- Features calculées
        safe_divide(Depense_de_sante, 100)              as part_sante_pib,
        safe_divide(Depense_publique_education, 100)    as part_education_pib,

        lag(PIB_par_habitant) over (
            partition by pays_code order by annee
        )                                               as PIB_par_habitant_n1,

        safe_divide(
            PIB_par_habitant - lag(PIB_par_habitant) over (
                partition by pays_code order by annee
            ),
            lag(PIB_par_habitant) over (
                partition by pays_code order by annee
            )
        ) * 100                                         as croissance_pib_hab_pct

    from source

)

select * from features
where PIB_par_habitant is not null