from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
import io

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2025, 4, 8),
  'retries': 1,
  'retry_delay': timedelta(minutes=5)
}

def start_task():
  print("Starting ETL process for HIV data warehouse")

def load_csv_to_postgres(file, table_name, **kwargs):
  import pandas as pd
  from sqlalchemy import create_engine
  from airflow.providers.postgres.hooks.postgres import PostgresHook

  try:
    df = pd.read_csv(file)
    print(f"Nombre de lignes lues dans {file}: {len(df)}")
    print(f"Colonnes originales: {df.columns.tolist()}")

    MAX_COLUMN_LENGTH = 60
    clean_columns = []
    column_dict = {}

    for i, col in enumerate(df.columns):
      clean_col = col.strip().replace(' ', '_').replace('(%)', 'pct')
      clean_col = clean_col[:MAX_COLUMN_LENGTH]
      if clean_col in column_dict:
        clean_col = f"{clean_col}_{i}"
      column_dict[clean_col] = True
      clean_columns.append(clean_col)
    
    df.columns = clean_columns
    print(f"Colonnnes apres nettoyage: {df.columns.tolist()}")
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=True)
    csv_buffer.seek(0)

    column_definitions = []
    for column in df.columns:
      if df[column].dtype == 'int64':
        column_definitions.append(f"{column} INTEGER")
      elif df[column].dtype == 'float64':
        column_definitions.append(f"{column} NUMERIC")
      else:
        column_definitions.append(f"{column} TEXT")
    columns_sql = ", ".join(column_definitions)
    pg_hook.run(f"DROP TABLE IF EXISTS {table_name};")
    create_table_sql = f"CREATE TABLE {table_name} ({columns_sql});"
    print(f"SQL pour creation de table: {create_table_sql}")
    pg_hook.run(create_table_sql)
    
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", csv_buffer)
    conn.commit()
    cur.close()

    print(f"Charge {file} dans la table {table_name}")
  except FileNotFoundError as er:
    raise ValueError(f"Le fichier {file} est introuvable: {str(er)}")
  except Exception as er:
    raise ValueError(f"Erreur lors du chargement de {file} dans {table_name}: {str(er)}")
  
def end_task():
  print("ETL process for HIV data warehouse completed")

with DAG('data_warehouse_etl',
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False
         ) as dag:
  
  start = PythonOperator(
    task_id='START',
    python_callable=start_task
  )

  extract_file1 = PythonOperator(
    task_id="EXTRACT_FILE_1",
    python_callable=load_csv_to_postgres,
    op_kwargs={
      'file': '../source_files/no_of_people_living_with_hiv_by_country_clean.csv',
      'table_name': 'HIV_population_source_table_1'
    }
  )

  extract_file2 = PythonOperator(
    task_id="EXTRACT_FILE_2",
    python_callable=load_csv_to_postgres,
    op_kwargs={
      'file': '../source_files/art_pediatric_coverage_by_country_clean.csv',
      'table_name': 'ART_children_source_table'
    }
  )

  extract_file3 = PythonOperator(
    task_id="EXTRACT_FILE_3",
    python_callable=load_csv_to_postgres,
    op_kwargs={
      'file': '../source_files/no_of_deaths_by_country_clean.csv',
      'table_name': 'HIV_deaths_source_table'
    }
  )

  extract_file4 = PythonOperator(
    task_id="EXTRACT_FILE_4",
    python_callable=load_csv_to_postgres,
    op_kwargs={
      'file': '../source_files/no_of_cases_adults_15_to_49_by_country_clean.csv',
      'table_name': 'HIV_prevalence_adults_source_table'
    }
  )

  extract_file5 = PythonOperator(
    task_id="EXTRACT_FILE_5",
    python_callable=load_csv_to_postgres,
    op_kwargs={
      'file': '../source_files/art_coverage_by_country_clean.csv',
      'table_name': 'ART_population_source_table'
    }
  )

  transform_dimensions = PostgresOperator(
    task_id="TRANSFORMATION_DIMENSIONS",
    postgres_conn_id='postgres_default',
    sql="""
      DELETE FROM fait_analyse_vih;
      
      -- Nettoyage des tables de dimension (optionnel, pour verifier les doublons)
      DELETE FROM dim_pays WHERE code_pays NOT IN (SELECT DISTINCT code_pays FROM fait_analyse_vih);
      DELETE FROM dim_region WHERE code_region NOT IN (SELECT DISTINCT code_region FROM dim_pays);;
      DELETE FROM  dim_date;

      ALTER SEQUENCE dim_pays_code_pays_seq RESTART;
      ALTER SEQUENCE dim_date_code_date_seq RESTART;
      ALTER SEQUENCE dim_region_code_region_seq RESTART;

      -- Insertion dans dim_region
      INSERT INTO Dim_Region (nom_region)
      SELECT DISTINCT who_region
      FROM (
        SELECT who_region FROM HIV_population_source_table_1
        UNION
        SELECT who_region FROM HIV_deaths_source_table
        UNION
        SELECT who_region FROM ART_children_source_table
        UNION
        SELECT who_region FROM HIV_prevalence_adults_source_table
        UNION
        SELECT who_region FROM ART_population_source_table
      ) AS combined
      WHERE who_region IS NOT NULL;

      --Insertion dans dim_pays
      INSERT INTO dim_pays(nom_pays, code_region)
      SELECT DISTINCT country, dr.code_region
      FROM (
        SELECT country, who_region FROM HIV_population_source_table_1
        UNION
        SELECT country, who_region FROM HIV_deaths_source_table
        UNION
        SELECT country, who_region FROM ART_children_source_table
        UNION
        SELECT country, who_region FROM HIV_prevalence_adults_source_table
        UNION
        SELECT country, who_region FROM ART_population_source_table
      ) AS combined
      JOIN dim_region dr ON dr.nom_region = combined.who_region
      WHERE country IS NOT NULL;

      INSERT INTO dim_date(annee)
      SELECT DISTINCT year 
      FROM (
        SELECT 2000 AS year UNION SELECT 2005 UNION SELECT 2010
        UNION
        SELECT 2000 AS year UNION SELECT 2010 UNION SELECT 2018
        UNION
        SELECT 2000 AS year UNION SELECT 2005 UNION SELECT 2010 UNION SELECT 2018
        UNION
        SELECT 2023 AS year
        UNION
        SELECT 2018 AS year
      ) AS combined;

      -- Logs pour validation
      DO $$
      BEGIN
          RAISE NOTICE 'Nombre de lignes dans dim_region : %', (SELECT COUNT(*) FROM dim_region);
          RAISE NOTICE 'Nombre de lignes dans dim_pays : %', (SELECT COUNT(*) FROM dim_pays);
          RAISE NOTICE 'Nombre de lignes dans dim_date : %', (SELECT COUNT(*) FROM dim_date);
      END $$;
    """
  )

  load_facts = PostgresOperator(
    task_id="LOADING_FACTS",
    postgres_conn_id='postgres_default',
    sql="""
      ALTER SEQUENCE Fait_ANALYSE_VIH_code_analyse_seq RESTART;
      WITH base_data AS (
                SELECT 
                    REGEXP_REPLACE(country, '\s*\([^)]+\)', '') AS country,
                    year AS year,
                    CASE WHEN source = 'malade' THEN value::FLOAT::INT ELSE NULL END AS nombre_malade,
                    CASE WHEN source = 'mort' THEN value::FLOAT::INT ELSE NULL END AS nombre_mort,
                    CASE WHEN source = 'traite' THEN value::FLOAT::INT ELSE NULL END AS nombre_traite,
                    CASE WHEN source = 'enfant_traite' THEN value::FLOAT::INT ELSE NULL END AS nombre_enfant_traite,
                    CASE WHEN source = 'adulte' THEN value::DECIMAL(5,1) ELSE NULL END AS nombre_adulte_15_49
                FROM (
                    SELECT country, year, 
                        CASE 
                            WHEN TRIM(REPLACE(Count_median::TEXT, ' ', '')) ~ '^[0-9.]+$' THEN REPLACE(Count_median::TEXT, ' ', '')
                            ELSE NULL 
                        END AS value,
                        'malade' AS source
                    FROM HIV_Population_Source_Table_1

                    UNION ALL

                    SELECT country, year, 
                        CASE 
                            WHEN TRIM(REPLACE(Reported_number_of_children_receiving_ART::TEXT, ' ', '')) ~ '^[0-9.]+$' THEN REPLACE(Reported_number_of_children_receiving_ART::TEXT, ' ', '')
                            ELSE NULL 
                        END AS value,
                        'enfant_traite'
                    FROM ART_Children_Source_Table

                    UNION ALL

                    SELECT country, year, 
                        CASE 
                            WHEN TRIM(REPLACE(Count_median::TEXT, ' ', '')) ~ '^[0-9.]+$' THEN REPLACE(Count_median::TEXT, ' ', '')
                            ELSE NULL 
                        END AS value,
                        'mort'
                    FROM HIV_Deaths_Source_Table

                    UNION ALL

                    SELECT country, year, 
                        CASE 
                            WHEN TRIM(REPLACE(Reported_number_of_people_receiving_ART::TEXT, ' ', '')) ~ '^[0-9.]+$' THEN REPLACE(Reported_number_of_people_receiving_ART::TEXT, ' ', '')
                            ELSE NULL 
                        END AS value,
                        'traite'
                    FROM ART_Population_Source_Table

                    UNION ALL

                    SELECT country, year, 
                        CASE 
                            WHEN TRIM(REPLACE(Count_median::TEXT, ' ', '')) ~ '^[0-9.]+$' THEN REPLACE(Count_median::TEXT, ' ', '')
                            ELSE NULL 
                        END AS value,
                        'adulte'
                    FROM HIV_Prevalence_Adults_Source_Table
                ) raw
            )

            INSERT INTO Fait_ANALYSE_VIH (
                Code_date,
                Code_pays,
                nombre_malade,
                nombre_mort,
                nombre_traite,
                nombre_enfant_traite,
                nombre_adulte_15_49
            )
            SELECT 
                dd.Code_date,
                dp.Code_pays,
                SUM(nombre_malade) AS nombre_malade,
                SUM(nombre_mort) AS nombre_mort,
                SUM(nombre_traite) AS nombre_traite,
                SUM(nombre_enfant_traite) AS nombre_enfant_traite,
                AVG(nombre_adulte_15_49) AS nombre_adulte_15_49
            FROM base_data bd
            JOIN Dim_Pays dp ON dp.Nom_pays = REGEXP_REPLACE(bd.country, '\s*\([^)]+\)', '')
            JOIN Dim_Date dd ON dd.annee = bd.year AND dd.Code_date = (SELECT MIN(Code_date) FROM Dim_Date WHERE annee = bd.year)
            GROUP BY dd.Code_date, dp.Code_pays;

            -- Ajouter des index pour améliorer les performances
            CREATE INDEX IF NOT EXISTS idx_fait_analyse_vih_code_date ON Fait_ANALYSE_VIH (Code_date);
            CREATE INDEX IF NOT EXISTS idx_fait_analyse_vih_code_pays ON Fait_ANALYSE_VIH (Code_pays);

            -- Log pour validation
            DO $$
            BEGIN
                RAISE NOTICE 'Nombre de lignes insérées dans Fait_ANALYSE_VIH : %', (SELECT COUNT(*) FROM Fait_ANALYSE_VIH);
            END $$;
    """
  )

  end = PythonOperator(
    task_id="END",
    python_callable=end_task
  )

  start >> [extract_file1, extract_file2, extract_file3, extract_file4, extract_file5] >> transform_dimensions >> load_facts >> end

