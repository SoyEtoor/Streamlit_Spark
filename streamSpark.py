from pymongo import MongoClient
import streamlit as st
import pandas as pd
import requests

CONFIG = {
    "JSONL_URL": "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl",
    "PRODUCER_URL": "https://kafka-postgres-producer-lyx5.onrender.com/send-crimes",
    "PRODUCER_AREA_URL": "https://kafka-mongo-producer-s73g.onrender.com/send-areas",
    "GITHUB_REPO_DEFAULT": "Streamlit_Spark",
    "GITHUB_USER_DEFAULT": "SoyEtoor",
    "COLLECTION_NAME": "people",
    "DB_NAME": "people"
}

def send_request(url, headers, payload):
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  
        return response.json(), None
    except requests.RequestException as e:
        return None, str(e)

def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    url = f'https://api.github.com/repos/{user}/{repo}/dispatches'
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github.v3+json',
        'Content-type': 'application/json'
    }
    payload = {
        "event_type": job,
        "client_payload": {
            "codeurl": codeurl,
            "dataseturl": dataseturl
        }
    }

    st.write(f"📡 Request: {url}")
    st.write("📦 Payload:", payload)
    
    result, error = send_request(url, headers, payload)
    
    if error:
        st.write(f"❌ Error: {error}")
    else:
        st.success("✅ Trabajo de Spark enviado correctamente!")
        st.info(f"📊 Mensaje: {result['message']}")

def process_crimes_to_kafka():
    st.spinner('🚀 Enviando datos al producer de Kafka...')
    result, error = send_request(CONFIG["PRODUCER_URL"], {}, {})
    if error:
        st.error(f"❌ Error: {error}")
    else:
        st.success("✅ Datos enviados correctamente a Kafka!")
        st.info(f"📊 Mensaje: {result['message']}")

def get_data_from_postgres():
    try:
        with st.spinner("🔌 Conectando a PostgreSQL..."):
            # Verificar si la conexión está configurada
            if "postgres" not in st.secrets:
                st.error("❌ Configuración 'neon' no encontrada en secrets")
                return
                
            # Establecer conexión con timeout
            conn = st.connection("postgres", type="sql")
            
            # Verificar si la tabla existe
            table_exists = conn.query(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'crimes');",
                ttl=600
            ).iloc[0,0]
            
            if not table_exists:
                st.error("❌ La tabla 'crimes' no existe en la base de datos")
                return
                
            # Obtener datos con límite para pruebas
            query = "SELECT * FROM crimes LIMIT 1000;"
            df = conn.query(query, ttl=600)
            
            if df.empty:
                st.warning("⚠️ La tabla 'crimes' está vacía")
                return
                
            # Mostrar datos
            st.success(f"✅ Obtenidos {len(df)} registros desde PostgreSQL 🐘")
            st.dataframe(df)
            
            # Mostrar métricas
            st.subheader("📊 Métricas de los datos")
            cols = st.columns(3)
            
            # Total registros
            cols[0].metric("Total registros", len(df))
            
            # Métricas condicionales
            if 'victim_age' in df.columns:
                avg_age = round(df['victim_age'].mean(), 1)
                cols[1].metric("Edad promedio víctimas", avg_age)
            else:
                cols[1].warning("No hay datos de edad")
                
            if 'crm_cd_desc' in df.columns:
                unique_crimes = df['crm_cd_desc'].nunique()
                cols[2].metric("Tipos de crimen", unique_crimes)
            else:
                cols[2].warning("No hay datos de tipos de crimen")
                
    except Exception as e:
        st.error(f"❌ Error crítico: {str(e)}")
        st.exception(e)  # Muestra el traceback completo

def process_area_to_kafka():
    st.spinner('🚀 Enviando datos de áreas al producer de Kafka...')
    result, error = send_request(CONFIG["PRODUCER_AREA_URL"], {}, {})
    if error:
        st.error(f"❌ Error: {error}")
    else:
        st.success("✅ Datos de áreas enviados correctamente a Kafka!")
        st.info(f"📊 Mensaje: {result['message']}")

def get_data_from_mongo():
    try:
        with st.spinner("📡 Conectando a MongoDB..."):
            client = MongoClient(st.secrets["mongodb"]["uri"])
            collection = client[CONFIG["DB_NAME"]][CONFIG["COLLECTION_NAME"]]
            data = list(collection.find())

            if not data:
                st.warning("⚠️ No se encontraron datos en MongoDB.")
                return

            df = pd.DataFrame(data)
            if '_id' in df.columns:
                df['_id'] = df['_id'].astype(str)

            st.success("✅ Datos obtenidos correctamente desde MongoDB 🍃")
            st.dataframe(df)

            st.subheader("📊 Métricas de los datos")
            st.metric("Total de documentos", len(df))
            if "crime_details" in df.columns and isinstance(df["crime_details"].iloc[0], dict):
                crime_df = pd.json_normalize(df["crime_details"])
                if "victim_age" in crime_df.columns:
                    st.metric("Edad promedio víctimas", round(crime_df["victim_age"].mean(), 1))
                if "crm_cd_desc" in crime_df.columns:
                    st.metric("Tipos de crimen", crime_df["crm_cd_desc"].nunique())

    except Exception as e:
        st.error(f"❌ Error al obtener datos desde MongoDB: {str(e)}")

st.title("🏢 BigData Dashboard - Sistema de Criminalidad")

tab1, tab2, tab3 = st.tabs(["Spark Jobs", "Kafka/PostgreSQL", "Kafka/MongoDB"])

with tab1:
    st.header("⚡ Submit Spark Job")
    with st.form("spark_form"):
        github_user = st.text_input("GitHub user", value=CONFIG["GITHUB_USER_DEFAULT"])
        github_repo = st.text_input("GitHub repo", value=CONFIG["GITHUB_REPO_DEFAULT"])
        spark_job = st.text_input("Spark job name", value="spark")
        github_token = st.text_input("GitHub token", value="", type="password")
        code_url = st.text_input("Code URL")
        dataset_url = st.text_input("Dataset URL")

        if st.form_submit_button("🚀 Ejecutar Spark Job"):
            post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)

with tab2:
    st.header("📊 Pipeline Kafka → PostgreSQL")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("🔄 Cargar datos a Kafka", key="kafka_crime"):
            process_crimes_to_kafka()

    with col2:
        if st.button("📥 Obtener datos de PostgreSQL"):
            get_data_from_postgres()

with tab3:
    st.header("📊 Pipeline Kafka → MongoDB")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("🔄 Cargar datos a Kafka", key="kafka_area"):
            process_area_to_kafka()

    with col2:
        if st.button("📥 Obtener datos de MongoDB"):
            get_data_from_mongo()
