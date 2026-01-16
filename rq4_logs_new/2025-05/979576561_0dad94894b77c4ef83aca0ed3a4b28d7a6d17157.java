import sys
import json
import mysql.connector
import numpy as np
from sentence_transformers import SentenceTransformer

# Configuraciones - AJUSTA según tu entorno
db_config = {
    'user': 'tu_usuario',
    'password': 'tu_password',
    'host': 'localhost',
    'database': 'tu_base_datos',
}

NOMBRE_MODELO_EMBEDDING = "local-sentence-transformers"
UMBRAL_SIMILITUD = 0.7

model = SentenceTransformer('all-MiniLM-L6-v2')

def get_modelo_id(conn, nombre):
    cursor = conn.cursor()
    cursor.execute("SELECT modeloId FROM MODELO WHERE nombre = %s", (nombre,))
    row = cursor.fetchone()
    cursor.close()
    if row:
        return row[0]
    else:
        raise ValueError(f"No se encontró modelo con nombre '{nombre}'")

def cargar_embeddings_etiquetas(conn, modeloId):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT etiquetaId, embedding
        FROM MODELO_EMBEDDING
        WHERE modeloId = %s
    """, (modeloId,))
    filas = cursor.fetchall()
    cursor.close()

    etiquetas = []
    embeddings = []
    for fila in filas:
        etiquetas.append(fila['etiquetaId'])
        vec = json.loads(fila['embedding'])
        embeddings.append(np.array(vec, dtype=np.float32))
    embeddings = np.vstack(embeddings) if embeddings else np.array([])
    return etiquetas, embeddings

def cargar_convocatoria(conn, convocatoriaId):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT convocatoriaId, titulo, texto
        FROM CONVOCATORIA
        WHERE convocatoriaId = %s
    """, (convocatoriaId,))
    fila = cursor.fetchone()
    cursor.close()
    if fila is None:
        raise ValueError(f"No existe convocatoria con convocatoriaId = {convocatoriaId}")
    return fila

def similaridad_coseno(v1, matriz):
    v1_norm = v1 / np.linalg.norm(v1)
    matriz_norm = matriz / np.linalg.norm(matriz, axis=1, keepdims=True)
    return np.dot(matriz_norm, v1_norm)

def insertar_actualizar_etiquetado(conn, convocatoriaId, etiquetaId, modeloId, confianza):
    cursor = conn.cursor()
    query = """
    INSERT INTO ETIQUETADO (convocatoriaId, etiquetaId, modeloId, confianza, createdAt, updatedAt)
    VALUES (%s, %s, %s, %s, NOW(), NOW())
    ON DUPLICATE KEY UPDATE confianza = %s, updatedAt = NOW()
    """
    cursor.execute(query, (convocatoriaId, etiquetaId, modeloId, confianza, confianza))
    cursor.close()

def main():
    if len(sys.argv) < 2:
        print("Uso: python script.py <convocatoriaId>")
        sys.exit(1)

    convocatoriaId = sys.argv[1]

    try:
        conn = mysql.connector.connect(**db_config)
        modeloId = get_modelo_id(conn, NOMBRE_MODELO_EMBEDDING)
        etiquetas, embeddings_etiquetas = cargar_embeddings_etiquetas(conn, modeloId)

        if len(etiquetas) == 0:
            print(f"No hay embeddings de etiquetas para el modelo {NOMBRE_MODELO_EMBEDDING}")
            sys.exit(1)

        convocatoria = cargar_convocatoria(conn, convocatoriaId)
        texto_concat = (convocatoria['titulo'] or '') + ' ' + (convocatoria['texto'] or '')
        emb_conv = model.encode(texto_concat, convert_to_numpy=True)

        sims = similaridad_coseno(emb_conv, embeddings_etiquetas)
        indices_relevantes = np.where(sims >= UMBRAL_SIMILITUD)[0]

        if len(indices_relevantes) == 0:
            # No se encontraron etiquetas relevantes
            sys.exit(1)

        for idx in indices_relevantes:
            etiquetaId = etiquetas[idx]
            confianza = float(sims[idx])
            insertar_actualizar_etiquetado(conn, convocatoriaId, etiquetaId, modeloId, confianza)

        conn.commit()
        sys.exit(0)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        try:
            conn.close()
        except:
            pass

if __name__ == '__main__':
    main()