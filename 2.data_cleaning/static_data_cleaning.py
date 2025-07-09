import polars as pl
import json
import os
import re

with open('/home/poette.m/dypo/params.json', 'r') as file :
    params = json.load(file)

DATASET, VERSION = params['dataset'], params['version']
DATA_FOLD = params['data_folder']
INPUT_FOLDER = f'{DATA_FOLD}/{VERSION}/1.raw_data/{DATASET}/'
OUTPUT_FOLDER = f'{DATA_FOLD}/{VERSION}/2.clean_data/{DATASET}/static/'
CENSUS_FILE = 'census/raw_census.xlsx'
IGS_FILE = 'igs/igs_all.csv'
OUTPUT_STATIC_FILE = 'clean_static_encounters_test_script.parquet'

def clean_percentage(value):
    # Probabilité de décès par IGS II en valeur décimale
    try:
        clean_value = float(value.replace('Mortalité prédite : ', '').replace('%', '').replace(',', '.')) / 100
        return clean_value
    except :
        return None

def clean_encounter(value):
    # Nettoyage des données sur la table des identifiants
    try:
        remove_letters = re.sub('\D', '', value)
        return remove_letters
    except:
        return None

def clean_igs(value):
    try:
        clean_value = float(value)
        return clean_value
    except:
        return None

raw_data = pl.read_excel(INPUT_FOLDER + CENSUS_FILE, schema_overrides={"lifeTimeNumber": pl.Int64}) # Données statiques brutes
df_igs = pl.read_csv(INPUT_FOLDER + IGS_FILE)

if DATASET == 'mimic' :
    raw_data = raw_data.rename(
        # Renommage des variables Mimic pour correspondre aux noms sur le dataset CHU
                    {
                        'displaylabel': 'displayLabel',
                        'encounterid': 'encounterId',
                        'encounternumber': 'encounterNumber',
                        'lifetimenumber': 'lifeTimeNumber',
                        'dateofdeath' : 'dateOfDeath',
                        'isdeceased' : 'isDeceased',
                        'utcintime' : 'utcInTime',
                        'utcouttime' : 'utcOutTime',
                        'lengthofstay' : 'lengthOfStay',
                        'height' : 'taille',
                        'weight' : 'poids_admission'
                    }
                ).with_columns(
                    # Création de colonnes vides pour les données nominatives de Mimic
                    [
                        pl.lit(None).alias('adresse'),
                        pl.lit(None).alias('ville'),
                        pl.lit(None).alias('cp'),
                        pl.lit(None).alias('dateOfBirth'),
                        pl.lit(None).alias('lastName'),
                        pl.lit(None).alias('firstName')
                    ]
                )

if DATASET == 'chu' :
    raw_data = raw_data.with_columns(
        # Création de colonnes vides pour les données de taille / poids des données CHU
                    [
                        pl.lit(None).alias('taille'),
                        pl.lit(None).alias('poids_admission'),
                    ]
    )

    df_insee = pl.read_csv(f'{DATA_FOLD}/{VERSION}/1.raw_data/chu/census/deces_insee.csv', ignore_errors=True)
    df_mortality= (df_insee.filter(pl.col("upper") == 1)
        .cast(
            {"date_deces" : pl.Date},
            strict=False
        )
        .with_columns(
                pl.when(pl.col('DECES_PASTEL') == "NA")
                .then(pl.lit(None))
                .otherwise(pl.lit(True))
                .alias("deces_hosp")
    )
        .sort(["NIP", "mean_score", "date_deces"], descending=[True, True, False])
        .unique("NIP", keep="first")
        .drop(
            ['id', 'probas_rf', 'probas_nn', 'upper','numero_acte_deces', 'mean_score', 'DECES_PASTEL' ]
            )
        .rename(
            {"NIP" : "lifeTimeNumber"}
        )
        )
    
if DATASET == 'chu' :
    icu_units = [ 
                'PURPAN REA. POLY.',
                'IUC REA.', 
                'NEURO-CHIR REA', 
                'RANGUEIL REA. POLY.',
                'RANGUEIL DECHO. REA.', 
                'PURPAN DECHO. REA.', 
                'PURPAN SC. REA.', 
                'RANGUEIL SC. REA.',
                'IUC SC.',
                'C.C.V. REA',
                'GRANDS BRULES',
                'NEURO-CHIR USC'
                ]
    
elif DATASET == 'mimic' :
    icu_units = [
                'Neuro Surgical Intensive Care Unit (Neuro SICU)',
                'Surgical Intensive Care Unit (SICU)',
                'Neuro Stepdown',
                'Medical/Surgical Intensive Care Unit (MICU/SICU)',
                'Cardiac Vascular Intensive Care Unit (CVICU)', #retrait chirurgie CCV pour correspondre au dataset CHU
                'Neuro Intermediate',
                'Coronary Care Unit (CCU)',
                'Medical Intensive Care Unit (MICU)',
                'Trauma SICU (TSICU)'
                ]

encounter_df = (raw_data
                .filter(
                    # Suppression des unités non désirées
                    pl.col('displayLabel').is_in(icu_units),
                    # Suppression des patients mineurs lors de leur admission
                    (pl.col('age') >= 18)
                    )
                .with_columns(
                    # Nettoyrage des encounterId
                    pl.col('encounterId').cast(pl.String).map_elements(clean_encounter, return_dtype=pl.String).alias('encounterId')
                    )
                .sort(by=['encounterId', 'utcInTime'])
                .group_by(
                    ['encounterId',
                    'encounterNumber',
                    'lifeTimeNumber',
                    'lastName',
                    'firstName',
                    'gender',
                    'age',
                    'dateOfBirth',
                    'lengthOfStay']
                )
                .agg([
                # Récupération des dates d'entrée minimales et de sortie maximale pour chaque séjour
                # Séléction de la première unité de réanimation 
                    pl.col('utcInTime').min().alias('utcInTime'),
                    pl.col('utcOutTime').max().alias('utcOutTime'),
                    pl.col("displayLabel")
                    .filter((pl.col("displayLabel").is_not_null()))
                    .sort_by('lengthOfStay', descending=True)
                    .first()
                    .alias('unitLabel'),
                    pl.col('adresse').max().alias('adresse'),
                    pl.col('ville').max().alias('ville'),
                    pl.col('cp').max().alias('cp'),
                    pl.col('taille').first().alias('taille'),
                    pl.col('poids_admission').first().alias('poids_admission')
                ])
                .join(
                    # Jointure sur le dataset de mortalité
                    df_mortality, on='lifeTimeNumber', how='left'
                ).with_columns(
                    [
                    # Durée de séjour en heures (date de sortie max - date d'entrée min)
                        ((((pl.col('utcOutTime').sub(pl.col('utcInTime'))).dt.total_minutes())/60)).round(2).alias('los')
                    ]
                ).with_columns(
                    # Survenue d'un décès défini comme la présence d'une date de décès ou du statut'décès' concernant le mode de sortie
                    # Si ni l'un ni l'autre = survie
                    pl.col('date_deces').is_not_null().alias("isDeceased"),
                    pl.col('deces_hosp').is_not_null().alias("deces_hosp"),
                    (pl.col('date_deces')- pl.col('utcInTime')).dt.total_days().alias('deces_datediff')
                ).drop('lengthOfStay')

            )

# Pour le Dataset CHU : ajout des motifs d'entrée et de sortie (en texte libre) et ajout du poids et de la taille

df_demo_extended = encounter_df
if DATASET == 'chu': 
    df_demo_extended = df_demo_extended.drop(['taille', 'poids_admission'])
    directory = INPUT_FOLDER + 'extended_demography/'
    for filename in os.listdir(directory):
        if filename.endswith(".parquet"): 
            df_extended_feature = pl.read_parquet(directory + filename)
            feature = df_extended_feature.get_column('feature').to_list()[0]
            df_extended_feature = df_extended_feature.sort(
                            'encounterId', 'utcChartTime'
                        ).unique(
                            subset=['encounterId'], keep='last'
                        )
            if feature in ['taille', 'poids_admission'] :
                df_extended_feature = df_extended_feature.rename(
                        {'valueNumber' : feature}
                )
            else:
                df_extended_feature = df_extended_feature.rename(
                        {'valueString' : feature}
                )
            df_extended_feature = df_extended_feature.select('encounterId', feature)
            df_demo_extended = df_demo_extended.join(
                            df_extended_feature, on='encounterId', how='left'
                        )
    ttt = pl.read_csv(INPUT_FOLDER + '/extended_demography/ttt.csv', separator=';').sort(by=['encounterId', 'storeTime']).unique('encounterId', keep='last')
    atcd = pl.read_csv(INPUT_FOLDER + '/extended_demography/atcd.csv', separator=';').sort(by=['encounterId', 'storeTime']).unique('encounterId', keep='last')
    ttt = ttt.select('encounterId', 'valueString').rename({'valueString' : 'traitements'}).cast({'encounterId' : pl.String})
    atcd = atcd.select('encounterId', 'valueString').rename({'valueString' : 'atcd'}).cast({'encounterId' : pl.String})
    df_demo_extended = df_demo_extended.join(ttt, on = 'encounterId', how='left').join(atcd, on = 'encounterId', how='left')

if DATASET == 'mimic' :
    df_igs_clean = (
            df_igs  
            .rename(
                {
                    'encounterid': 'encounterId'
                    ,'admissiontype_score' : 'admission_type'
                }
            )
            .sort('encounterId', 'sapsii', descending=[False, True])
            .unique('encounterId', keep='first')
            .with_columns(
                pl.when(pl.col('admission_type') == 8)
                    .then(pl.lit('Unscheduled Surgery'))
                .when(pl.col('admission_type') == 0)
                    .then(pl.lit('Scheduled Surgery'))
                .when(pl.col('admission_type') == 6)
                    .then(pl.lit('Medical'))
                .otherwise(None).alias('admission_type')
                )
            .cast({'encounterId': pl.String})
            .select('encounterId', 'admission_type', 'sapsii', 'sapsii_prob')
            )
    df_demo_extended = df_demo_extended.join(df_igs_clean, on='encounterId', how='left')

if DATASET == 'chu' : 
    df_igs_clean = (
                df_igs  
                .with_columns(
                        pl.col('encounterNumber').map_elements(clean_encounter, return_dtype=pl.String).alias('encounterNumber'),
                        pl.col('igsMort').map_elements(clean_percentage, return_dtype=pl.Float64).alias('sapsii_prob')
                        )
                .sort('encounterNumber', 'igsStoreTime')
                .unique('encounterNumber', keep='first')
                .with_columns(
                    pl.when(pl.col('igsTypeAdm') == 0)
                        .then(pl.lit('Medical'))
                    .when(pl.col('igsTypeAdm') == 2)
                        .then(pl.lit('Unscheduled Surgery'))
                    .when(pl.col('igsTypeAdm') == 1)
                        .then(pl.lit('Scheduled Surgery'))
                    .otherwise(None).alias('admission_type')
                    )
                .rename(
                    {
                    'igsTotal' : 'sapsii',
                    'igsGlgw' : 'score_glasgow'
                    }
                )
                .select('encounterNumber', 'admission_type', 'sapsii', 'sapsii_prob', 'score_glasgow')
                .cast(
                    {'encounterNumber' : pl.Int64}, 
                    strict = False
                )
                )
    
    df_demo_extended = df_demo_extended.join(df_igs_clean, on='encounterNumber', how='left')

    col_identifiantes = [
    'encounterNumber',
    'lifeTimeNumber',
    'lastName',
    'firstName',
    'dateOfBirth',
    'cp',
    'ville',
    'adresse',
    'utcInTime',
    'utcOutTime',
    'date_deces',
    'code_lieu_deces',
]

df_demo_extended.write_parquet(OUTPUT_FOLDER + OUTPUT_STATIC_FILE)

if DATASET == 'chu' : 
    df_indexed = df_demo_extended.with_row_index(offset=1)
    table_corr = df_indexed.select(col_identifiantes)
    df_pseudonymised = (
        df_indexed
            .with_columns(
                year_inTime = pl.col('utcInTime').dt.year()
            )
            .select(
                pl.exclude(col_identifiantes)
                )
        )
    table_corr.write_csv(OUTPUT_FOLDER + 'correlation_table_test_script.csv')
    df_pseudonymised.write_parquet(OUTPUT_FOLDER + 'clean_pseudonimysed_dataset_test_script.parquet')

print('script ok')