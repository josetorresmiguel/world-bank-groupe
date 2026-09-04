-- models/intermediate/int_indicateurs.sql

with stg as (
    select * from {{ ref('stg_indicateurs') }}
),

pivote as (
    select
        pays_code,
        max(pays_nom) as pays_nom,
        annee,

        max(case when indicateur_code = 'NY.GDP.MKTP.CD'       then valeur end) as PIB,
        max(case when indicateur_code = 'NY.GDP.PCAP.CD'       then valeur end) as PIB_par_habitant,
        max(case when indicateur_code = 'NY.GDP.MKTP.KD.ZG'    then valeur end) as Croissance_PIB,
        max(case when indicateur_code = 'SP.POP.TOTL'          then valeur end) as Population,
        max(case when indicateur_code = 'SP.DYN.LE00.IN'       then valeur end) as Esperance_vie,
        max(case when indicateur_code = 'SL.UEM.TOTL.ZS'       then valeur end) as Chomage,
        max(case when indicateur_code = 'FP.CPI.TOTL.ZG'       then valeur end) as Inflation,
        max(case when indicateur_code = 'EG.ELC.ACCS.ZS'       then valeur end) as Acces_electricite,
        max(case when indicateur_code = 'EN.GHG.CO2.PC.CE.AR5' then valeur end) as CO2_par_habitant,
        max(case when indicateur_code = 'SP.DYN.CBRT.IN'       then valeur end) as Taux_natalite,
        max(case when indicateur_code = 'SE.SEC.ENRR'          then valeur end) as Scolarisation_secondaire,
        max(case when indicateur_code = 'SE.TER.ENRR'          then valeur end) as Scolarisation_superieur,
        max(case when indicateur_code = 'SE.PRM.CMPT.ZS'       then valeur end) as Achevement_primaire,
        max(case when indicateur_code = 'SH.XPD.CHEX.GD.ZS'    then valeur end) as Depense_de_sante,
        max(case when indicateur_code = 'SE.XPD.TOTL.GD.ZS'    then valeur end) as Depense_publique_education,
        max(case when indicateur_code = 'SE.ADT.LITR.ZS'       then valeur end) as Alphabetisation_adultes

    from stg
    group by pays_code, annee
)

select * from pivote