from pymongo import MongoClient
import streamlit as st
import pandas as pd
import requests
import json

# Configuración personalizada para tu proyecto
CONFIG = {
    "JSON_URL": "https://raw.githubusercontent.com/AntonioIR1218/spark-labs/refs/heads/main/results/acoustic_tracks.json",
    "PRODUCER_URL": "https://producer-postgres.onrender.com/send-tracks",
    "PRODUCER_ACOUSTIC_URL": "https://producer-mongodb.onrender.com/send-tracks",
    "GITHUB_REPO_DEFAULT": "spark-labs",
    "GITHUB_USER_DEFAULT": "AntonioIR1218",
    "COLLECTION_NAME": "ExplicitEnergyTracks",
    "DB_NAME": "musica"
}

def send_request(url, headers=None, payload=None):
    """Función genérica para enviar requests HTTP"""
    try:
        response = requests.post(url, json=payload, headers=headers or {})
        response.raise_for_status()  
        return response.json(), None
    except requests.RequestException as e:
        return None, str(e)

def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    """Envía un trabajo Spark a GitHub Actions - Versión simplificada con debug"""
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
    
    # Mostrar información de depuración
    st.write("### Detalles de la solicitud")
    st.write("**URL:**", url)
    st.write("**Headers:**", headers)
    st.write("**Payload:**", payload)
    
    # Hacer la solicitud POST
    response = requests.post(url, json=payload, headers=headers)
    
    # Mostrar la respuesta
    st.write("**Respuesta del servidor:**")
    st.write("Status Code:", response.status_code)
    st.write("Response Text:", response.text)
    
    if response.status_code == 204:
        st.success("✅ Trabajo de Spark enviado correctamente!")
        st.balloons()
    else:
        st.error(f"❌ Error al enviar trabajo Spark: {response.text}")

def get_spark_results(url_results):
    """Obtiene resultados del job de Spark"""
    response = requests.get(url_results)
    st.write("**Respuesta del servidor:**")
    st.write("Status Code:", response.status_code)
    
    if response.status_code == 200:
        try:
            results = response.json()
            st.success("✅ Resultados obtenidos correctamente!")
            
            # Convertir a DataFrame si es posible
            if isinstance(results, list):
                df = pd.DataFrame(results)
                st.dataframe(df)
            
            st.json(results)
        except ValueError:
            st.write("Contenido:", response.text)
    else:
        st.error(f"❌ Error al obtener resultados: {response.text}")

# [Mantén todas las demás funciones exactamente igual]
def process_tracks_to_kafka_postgres():
    """Envía datos al producer de Kafka para PostgreSQL"""
    with st.status("Procesando datos..."):
        st.write("🌐 Conectando con Producer Postgres...")
        result, error = send_request(CONFIG["PRODUCER_URL"])
        
        if error:
            st.error(f"❌ Error: {error}")
        else:
            st.success("✅ Datos enviados correctamente a Kafka (Postgres)!")
            st.info(f"📊 Respuesta: {result.get('message', '')}")

def get_data_from_postgres():
    """Obtiene datos desde PostgreSQL"""
    try:
        with st.status("Conectando a PostgreSQL..."):
            conn = st.connection("postgres", type="sql")
            query = "SELECT * FROM acoustic_tracks;"
            df = conn.query(query, ttl=600)

            if df.empty:
                st.warning("⚠️ No se encontraron datos en PostgreSQL")
                return

            st.success(f"✅ Obtenidos {len(df)} registros desde PostgreSQL 🐘")
            st.dataframe(df)

            st.subheader("📊 Métricas de pistas acústicas")
            cols = st.columns(3)
            cols[0].metric("Total pistas", len(df))
            cols[1].metric("Duración promedio", f"{round(df['duration_ms'].mean()/1000,1)} seg")
            cols[2].metric("Explícitas", f"{df['explicit'].sum()} ({df['explicit'].mean()*100:.1f}%)")

    except Exception as e:
        st.error(f"⚠️ Error al conectar con PostgreSQL: {str(e)}")

def process_tracks_to_kafka_mongo():
    """Envía datos al producer de Kafka para MongoDB"""
    with st.status("Procesando datos..."):
        st.write("🌐 Conectando con Producer MongoDB...")
        result, error = send_request(CONFIG["PRODUCER_ACOUSTIC_URL"])
        
        if error:
            st.error(f"❌ Error: {error}")
        else:
            st.success("✅ Datos enviados correctamente a Kafka (MongoDB)!")
            st.info(f"📊 Respuesta: {result.get('message', '')}")

def get_data_from_mongo():
    try:
        with st.status("🔍 Conectando a MongoDB...", expanded=True) as status:
            # 1. Conexión
            client = MongoClient(st.secrets["mongodb"]["uri"])
            st.success("✅ Conexión exitosa a MongoDB Atlas")
            
            # 2. Verificar DB
            db = client[CONFIG["DB_NAME"]]
            
            # 3. Verificar colección
            collection = db[CONFIG["COLLECTION_NAME"]]
            
            # 4. Obtener TODOS los datos (sin límite)
            all_data = list(collection.find({}))
            
            if not all_data:
                status.update(label="⚠️ La colección existe pero está vacía", state="complete")
                return
                
            # 5. Convertir a DataFrame
            df = pd.DataFrame(all_data)
            
            # Eliminar columna _id si existe
            if '_id' in df.columns:
                df = df.drop(columns=['_id'])
            
            status.update(label=f"✅ Obtenidos {len(df)} documentos", state="complete")
            
            # 6. Mostrar en una sección ampliada
            st.subheader("📊 Datos completos", divider="rainbow")
            
            # Configurar el dataframe para que ocupe más espacio
            st.dataframe(
                df,
                height=600,  # Altura aumentada
                width=1200,  # Ancho aumentado
                use_container_width=True,
                hide_index=True
            )
            
            # 7. Mostrar estadísticas detalladas
            st.subheader("📈 Estadísticas completas")
            
            cols = st.columns(4)
            cols[0].metric("Total documentos", len(df))
            
            if 'duration_ms' in df.columns:
                duration_min = df['duration_ms'].min()/1000
                duration_max = df['duration_ms'].max()/1000
                duration_avg = df['duration_ms'].mean()/1000
                cols[1].metric("Duración (seg)", 
                              f"{duration_avg:.1f} (avg)",
                              delta=f"{duration_min:.1f}-{duration_max:.1f}")
            
            if 'explicit' in df.columns:
                explicit_count = df['explicit'].sum()
                explicit_percent = (explicit_count/len(df))*100
                cols[2].metric("Contenido explícito", 
                              f"{explicit_count} ({explicit_percent:.1f}%)")
            
            if 'artists' in df.columns:
                unique_artists = df['artists'].explode().nunique()
                cols[3].metric("Artistas únicos", unique_artists)
            
            # 8. Mostrar estructura de datos
            with st.expander("🔍 Estructura completa de los datos", expanded=True):
                st.json(all_data[0] if all_data else {})
                
            # 9. Opción para descargar los datos
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="⬇️ Descargar datos como CSV",
                data=csv,
                file_name='acoustic_tracks.csv',
                mime='text/csv'
            )

    except Exception as e:
        st.error(f"❌ Error crítico: {str(e)}")
        st.info("""
        Verifica:
        1. Tu conexión a internet
        2. El archivo secrets.toml
        3. Los permisos en Atlas
        4. Que la colección tenga datos
        """)

# Interfaz de la aplicación
st.set_page_config(page_title="Dashboard Musical", layout="wide")
st.title("🎵 Acoustic Tracks Analytics Dashboard")

tab1, tab2, tab3 = st.tabs(["Spark Jobs", "Kafka/PostgreSQL", "Kafka/MongoDB"])

with tab1:
    st.header("⚡ Gestión de Trabajos Spark")
    
    # Formulario simplificado para Spark
    col1, col2 = st.columns(2)
    with col1:
        github_user = st.text_input('GitHub user', value=CONFIG["GITHUB_USER_DEFAULT"])
        github_repo = st.text_input('GitHub repo', value=CONFIG["GITHUB_REPO_DEFAULT"])
        spark_job = st.text_input('Spark job name', value='spark')
    
    with col2:
        github_token = st.text_input('GitHub token', type='password')
        code_url = st.text_input('Code URL', 
                               value='https://raw.githubusercontent.com/AntonioIR1218/spark-labs/refs/heads/main/musica.py')
        dataset_url = st.text_input('Dataset URL', 
                                  value='https://raw.githubusercontent.com/AntonioIR1218/spark-labs/refs/heads/main/dataset.csv')
    
    if st.button("🚀 Ejecutar Spark Job", key="spark_submit"):
        post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)
    
    st.divider()
    st.header("📊 Resultados del Spark Job")
    url_results = st.text_input('URL de resultados', 
                              value=CONFIG["JSON_URL"])
    
    if st.button("🔄 Obtener Resultados", key="get_results"):
        get_spark_results(url_results)

with tab2:
    st.header("📊 Pipeline: Kafka → PostgreSQL")
    st.caption("Envía y visualiza datos de pistas acústicas en PostgreSQL")
    
    cols = st.columns([1, 1, 2])
    with cols[0]:
        if st.button("🔄 Cargar datos a Kafka", key="kafka_postgres", use_container_width=True):
            process_tracks_to_kafka_postgres()
    
    with cols[1]:
        if st.button("📥 Obtener datos de PostgreSQL", use_container_width=True):
            get_data_from_postgres()
    
   

with tab3:
    st.header("📊 Pipeline: Kafka → MongoDB")
    st.caption("Envía y visualiza datos de pistas acústicas en MongoDB")
    
    cols = st.columns([1, 1, 2])
    with cols[0]:
        if st.button("🔄 Cargar datos a Kafka", key="kafka_mongo", use_container_width=True):
            process_tracks_to_kafka_mongo()
    
    with cols[1]:
        if st.button("📥 Obtener datos de MongoDB", use_container_width=True):
            get_data_from_mongo()

# Notas adicionales
st.sidebar.markdown("## 🔍 Acerca de")
st.sidebar.info("""
Este dashboard permite:
- Procesar datos musicales con Spark
- Distribuir datos a través de Kafka
- Almacenar en PostgreSQL y MongoDB
- Visualizar métricas clave
""")

st.sidebar.markdown("## ⚙️ Configuración")
st.sidebar.write(f"**Repositorio:** {CONFIG['GITHUB_REPO_DEFAULT']}")
st.sidebar.write(f"**Dataset:** [acoustic_tracks.json]({CONFIG['JSON_URL']})")
