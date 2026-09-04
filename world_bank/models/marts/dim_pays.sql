with source as (

            select * from {{ ref('int_indicateurs') }}

),

agrege as (

    select
        pays_code,
        max(pays_nom) as pays_nom,
        max(annee)    as derniere_annee,

        max(PIB)                     as PIB,
        max(PIB_par_habitant)        as PIB_par_habitant,
        max(Population)              as Population,
        max(Esperance_vie)           as Esperance_vie,
        max(Chomage)                 as Chomage,
        max(Acces_electricite)       as Acces_electricite,
        max(Alphabetisation_adultes) as Alphabetisation_adultes,
        max(Depense_de_sante)        as Depense_de_sante,

        count(*) as nb_annees_observees

    from source
    where annee >= 2000
    group by pays_code

)

select * from agrege