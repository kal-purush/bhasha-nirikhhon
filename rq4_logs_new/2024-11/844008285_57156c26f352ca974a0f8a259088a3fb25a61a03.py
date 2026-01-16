import chromadb
import logging
from embeddings_utils import get_text_embedding

def get_or_create_vector_db(vdb_name, cname):
    client = chromadb.PersistentClient(path=vdb_name)
    collection = client.get_or_create_collection(name=cname)

    return collection    

def chromadb_store_data(vdb, ids, metadatas, documents, embeddings=None):
    for i in range(len(ids)):
        vdb.upsert(
            ids=ids[i],
            metadatas=metadatas[i],
            documents=documents[i],
            embeddings=embeddings[i],
        )
    return True

def vectordb_store_page_embeddings(collection, page_embeddings):
    for page in page_embeddings:
        collection.upsert(
            documents=[page["text"]],
            metadatas=[{"page_number": page["page_number"]}],
            ids=[f"page-{page['page_number']}"],
            embeddings=[page["embedding"]]
        )
    logging.info(f"Collection '{collection.name}' successfully updated.")

def store_embeddings_in_vectordb(collection, data):
    logging.debug("Storing page embeddings into ChromaDB")

    ids = [data['level'][e] for e in data['level']]
    documents = [data['text'][e] for e in data['text']]
    metadata = [{"key": key, "value": value} for key, value in data['level'].items()]
    embeddings = [data['embedding'][e] for e in data['embedding']]

    logging.info(f"len ids        :{len(ids)}")
    logging.info(f"len documents  :{len(documents)}")
    logging.info(f"len metadata   :{len(metadata)}")
    logging.info(f"len embeddings :{len(embeddings)}")

    collection.upsert(ids=ids, documents=documents, metadatas=metadata, embeddings=embeddings)
    logging.info(f"Collection '{collection.name}' successfully updated.")
    return

def pdf_store_embeddings_in_vectordb(collection, data):
    logging.debug("Storing page embeddings into ChromaDB")

    ids = [data['file-id']]
    documents = [data['file-text']]
    metadata = [{'key': data['file-id'], 'value': data['file-text']}]
    embeddings = [data['file-embedding']]

    for e in data['pages']:
        ids.append(f"{e['page-id']}")
        documents.append(f"{e['page-text']}")
        metadata.append({'key': f"{e['page-id']}, 'value': {e['page-text']}"})
        embeddings.append(e['page-embedding'])

    for e in data['chunks']:
        ids.append(f"{e['chunk-id']}")
        documents.append(f"{e['chunk-text']}")
        metadata.append({'key': f"{e['chunk-id']}", 'value': f"{e['chunk-text']}"})
        embeddings.append(e['chunk-embedding'])

    logging.info(f"len ids        :{len(ids)}")
    logging.info(f"len documents  :{len(documents)}")
    logging.info(f"len metadata   :{len(metadata)}")
    logging.info(f"len embeddings :{len(embeddings)}")

    collection.upsert(ids=ids, documents=documents, metadatas=metadata, embeddings=embeddings)
    logging.info(f"Collection '{collection.name}' successfully updated.")
    return
    
def vdb_search_by_id(collection, id_value=None):
    results = collection.get(ids=[id_value], include=["documents", "metadatas", "embeddings"])
    return results

def vdb_search_by_query(collection, query_text=None):
    query_embedding = get_text_embedding(query_text, 5)
    return collection.query(query_embeddings=[query_embedding], n_results=5)

def initialize_vector_db(vdb_name, cname):
    logging.debug(f"Initializing ChromaDB PersistentClient with path: {vdb_name}")
    client = chromadb.PersistentClient(path=vdb_name)
    collection = client.get_or_create_collection(name=cname)
    return collection