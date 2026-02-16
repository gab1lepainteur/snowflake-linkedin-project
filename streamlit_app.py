import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

try:
    session = get_active_session()
except:
    st.error("Cette application doit être exécutée dans Snowflake (Snowsight).")
    st.stop()

st.title("📊 Analyse des Offres d'Emploi LinkedIn")

# --- Analyse 1 : Top 10 titres par industrie ---
st.header("1. Top 10 des titres de postes les plus publiés par industrie")

query1 = """
    WITH ranked_jobs AS (
        SELECT
            ci.industry,
            jp.title,
            COUNT(*) AS nb_offres,
            ROW_NUMBER() OVER (PARTITION BY ci.industry ORDER BY COUNT(*) DESC) AS rn
        FROM job_postings jp
        JOIN companies c ON TRY_TO_NUMBER(jp.company_name) = c.company_id
        JOIN company_industries ci ON c.company_id = ci.company_id
        GROUP BY ci.industry, jp.title
    )
    SELECT industry, title, nb_offres
    FROM ranked_jobs
    WHERE rn <= 10
    ORDER BY industry, nb_offres DESC
"""
df1 = session.sql(query1).to_pandas()

if not df1.empty:
    selected_industry = st.selectbox("Choisir une industrie", df1['INDUSTRY'].unique())
    filtered = df1[df1['INDUSTRY'] == selected_industry].sort_values(by='NB_OFFRES', ascending=False)
    st.bar_chart(filtered.set_index('TITLE')['NB_OFFRES'])
else:
    st.info("Aucune donnée disponible pour l'analyse 1.")

# --- Analyse 2 : Top 10 postes mieux rémunérés ---
st.header("2. Top 10 des postes les mieux rémunérés par industrie")

query2 = """
    WITH ranked_salaries AS (
        SELECT
            ci.industry,
            jp.title,
            jp.max_salary,
            jp.pay_period,
            ROW_NUMBER() OVER (PARTITION BY ci.industry ORDER BY jp.max_salary DESC NULLS LAST) AS rn
        FROM job_postings jp
        JOIN companies c ON TRY_TO_NUMBER(jp.company_name) = c.company_id
        JOIN company_industries ci ON c.company_id = ci.company_id
        WHERE jp.max_salary IS NOT NULL
    )
    SELECT industry, title, max_salary, pay_period
    FROM ranked_salaries
    WHERE rn <= 10
    ORDER BY industry, max_salary DESC
"""
df2 = session.sql(query2).to_pandas()

if not df2.empty:
    selected_industry2 = st.selectbox("Industrie (salaires)", df2['INDUSTRY'].unique(), key='ind2')
    filtered2 = df2[df2['INDUSTRY'] == selected_industry2].sort_values(by='MAX_SALARY', ascending=False)
    st.bar_chart(filtered2.set_index('TITLE')['MAX_SALARY'])
else:
    st.info("Aucune donnée disponible pour l'analyse 2.")

# --- Analyse 3 : Répartition par taille d'entreprise ---
st.header("3. Répartition des offres par taille d'entreprise")

query3 = """
    SELECT
        CASE c.company_size
            WHEN 0 THEN '1-10'
            WHEN 1 THEN '11-50'
            WHEN 2 THEN '51-200'
            WHEN 3 THEN '201-500'
            WHEN 4 THEN '501-1000'
            WHEN 5 THEN '1001-5000'
            WHEN 6 THEN '5001-10000'
            WHEN 7 THEN '10000+'
            ELSE 'Inconnu'
        END AS taille_entreprise,
        COUNT(*) AS nb_offres
    FROM job_postings jp
    JOIN companies c ON TRY_TO_NUMBER(jp.company_name) = c.company_id
    GROUP BY c.company_size
    ORDER BY c.company_size
"""
df3 = session.sql(query3).to_pandas()

if not df3.empty:
    st.bar_chart(df3.set_index('TAILLE_ENTREPRISE')['NB_OFFRES'])
else:
    st.warning("Aucune donnée trouvée pour l'analyse 3 (jointure entreprise vide).")

# --- Analyse 4 : Répartition par secteur d'activité ---
st.header("4. Répartition des offres par secteur d'activité")

query4 = """
    SELECT ci.industry, COUNT(*) AS nb_offres
    FROM job_postings jp
    JOIN companies c ON TRY_TO_NUMBER(jp.company_name) = c.company_id
    JOIN company_industries ci ON c.company_id = ci.company_id
    GROUP BY ci.industry
    ORDER BY nb_offres DESC
    LIMIT 20
"""
df4 = session.sql(query4).to_pandas()

if not df4.empty:
    df4 = df4.sort_values(by='NB_OFFRES', ascending=False)
    st.bar_chart(df4.set_index('INDUSTRY')['NB_OFFRES'])

# --- Analyse 5 : Répartition par type d'emploi ---
st.header("5. Répartition des offres par type d'emploi")

query5 = """
    SELECT
        formatted_work_type,
        COUNT(*) AS nb_offres,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pourcentage
    FROM job_postings
    WHERE formatted_work_type IS NOT NULL
    GROUP BY formatted_work_type
    ORDER BY nb_offres DESC
"""
df5 = session.sql(query5).to_pandas()

if not df5.empty:
    df5 = df5.sort_values(by='NB_OFFRES', ascending=False)
    st.bar_chart(df5.set_index('FORMATTED_WORK_TYPE')['NB_OFFRES'])
    st.dataframe(df5)
