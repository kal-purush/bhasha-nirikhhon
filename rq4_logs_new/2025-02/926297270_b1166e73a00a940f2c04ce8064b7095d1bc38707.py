# Installing Dependencies
import os
import json
import streamlit as st
import gspread
import time
import pytz
import uuid
import random
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from langchain_core.runnables import (
    RunnableBranch,
    RunnableLambda,
    RunnableParallel,
    RunnablePassthrough,
)
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.prompts.prompt import PromptTemplate
from langchain_core.runnables import ConfigurableField
from neo4j import GraphDatabase
from langchain_community.vectorstores import Neo4jVector
from langchain_community.graphs import Neo4jGraph
from neo4j import GraphDatabase
from typing import Tuple, List, Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict
from langchain_community.vectorstores.neo4j_vector import remove_lucene_chars
from langchain_nomic import NomicEmbeddings
from langchain_ollama import ChatOllama
from langchain_groq import ChatGroq
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv
from langchain_community.embeddings import JinaEmbeddings
from langchain.retrievers import ContextualCompressionRetriever
from langchain_community.document_compressors import JinaRerank

st.set_page_config(
    page_title="PPKS | Chat Bot",
    page_icon="assets/ELA 1x1.jpg",
    layout="centered",
)


# Load environtment app
load_dotenv()

# Start Counter
start_counter = time.perf_counter()

# Setup a session state to hold up all the old messages
if 'messages_product_knowledge' not in st.session_state:
    st.session_state.messages_product_knowledge = []

if '_log' not in st.session_state:
    st.session_state['_log'] = []

if 'chat_history_product_knowledge' not in st.session_state:
    st.session_state.chat_history_product_knowledge = []

if 'need_greetings_product_knowledge' not in st.session_state:
    st.session_state.need_greetings_product_knowledge = True

if 'convert_status' not in st.session_state:
    st.session_state.convert_status = None

if 'conversion_done' not in st.session_state:
    st.session_state.conversion_done = None

if 'conversion_running' not in st.session_state:
    st.session_state.conversion_running = None

if 'idx_llm' not in st.session_state:
    st.session_state['idx_llm'] = 0

if 'total_time' not in st.session_state:
    st.session_state['total_time'] = 0

# st.write(st.session_state.convert_status)
if st.session_state.conversion_done is not None:
    if st.session_state.conversion_done:
        st.toast("Document conversion finished!", icon="✅")  # Or use st.success
        st.session_state.conversion_done = False  # Reset to avoid repeated toasts

if 'current_main_key_idx' not in st.session_state:
    st.session_state.current_main_key_idx = random.randint(0, 4)  # Pilih main key secara acak

if 'use_backup' not in st.session_state:
    st.session_state.use_backup = False

if 'backup_idx' not in st.session_state:
    st.session_state.backup_idx = -1

def remove_lucene_chars_cust(text: str) -> str:
    """Remove Lucene special characters"""
    special_chars = [
        "+",
        "-",
        "&",
        "|",
        "!",
        "(",
        ")",
        "{",
        "}",
        "[",
        "]",
        "^",
        '"',
        "~",
        "*",
        "?",
        ":",
        "\\",
        "/"
    ]

    for char in special_chars:
        if char in text:
            # if char == "/":
            #     text = text.replace(char, "\\/")
            # else :
            text = text.replace(char, " ")
    
    return text.strip()


# Load llm model using Groq
@st.cache_resource
def load_llm_groq(KEY):
    return ChatGroq(
        model='llama-3.1-70b-versatile', #llama-3.1-70b-versatile, llama-3.1-8b-instant
        temperature=0,
        api_key=KEY
    )

# # Get list main api keys
# main_keys = [
#     st.secrets['groq_key']['groq_1'],
#     st.secrets['groq_key']['groq_2'],
#     st.secrets['groq_key']['groq_3'],
#     st.secrets['groq_key']['groq_4'],
#     st.secrets['groq_key']['groq_5'],
#     # st.secrets['groq_key']['groq_6'],
#     # st.secrets['groq_key']['groq_7'],
#     # st.secrets['groq_key']['groq_8'],
#     # st.secrets['groq_key']['groq_9'],
#     # st.secrets['groq_key']['groq_10'],
# ]

# # Get list backup api keys
# backup_keys = [
#     st.secrets['groq_key']['groq_11'],
#     st.secrets['groq_key']['groq_12'],
#     st.secrets['groq_key']['groq_13'],
#     st.secrets['groq_key']['groq_14'],
#     # st.secrets['groq_key']['groq_15'],
#     # st.secrets['groq_key']['groq_16'],
#     # st.secrets['groq_key']['groq_17'],
#     # st.secrets['groq_key']['groq_18'],
#     # st.secrets['groq_key']['groq_19'],
#     # st.secrets['groq_key']['groq_20'],

# ]

# # Determine the api key currently in use
# if not st.session_state.use_backup:
#     current_key = main_keys[st.session_state.current_main_key_idx]
# else:
#     current_key = backup_keys[st.session_state.backup_idx]


# llms_groq = [load_llm_groq(st.secrets['groq_key']['groq_1']), load_llm_groq(st.secrets['groq_key']['groq_2']), load_llm_groq(st.secrets['groq_key']['groq_3']), load_llm_groq(st.secrets['groq_key']['groq_4']), load_llm_groq(st.secrets['groq_key']['groq_5'])]

# random_idx = random.randint(0, 4)
# llm_groq = llms_groq[random_idx]

llm_groq = load_llm_groq(st.secrets['groq_key']['groq_1'])

@st.cache_resource
def connect_to_google_sheets():
    # Define the scope
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    # Authenticate credentials
    credentials_dict = st.secrets["gspread_credential"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    client = gspread.authorize(creds)
    return client

# Save feedback to Google Sheets
def save_feedback_to_google_sheets(name, rating, feedback, chat_message):
    # Connect to Google Sheets
    client = connect_to_google_sheets()
    sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/12E4rDwSjblz-eDY6xiBpOECd0a8dwqOR1qQWsyXP1F4/edit?usp=sharing").sheet1# Open the Google Sheet by name
    
    chats = []
    comma = ",\n"

    if len(chat_message) <= 1:
        conversation = rf""
    else:
        for chat in chat_message[1:]:
            role = chat["role"]
            content = chat["content"]
            chats.append(
                f"{role}:{content}"
            )

        conversation = f"""
{comma.join([_chat for _chat in chats])}
"""
    # print(conversation)
    # Append the feedback
    sheet.append_row([datetime.now(pytz.timezone("Asia/Jakarta")).strftime("%Y-%m-%d %H:%M:%S"), name, rating, feedback, conversation])

# Load knowledge graph fron neo4j
@st.cache_resource
def load_knowledge_graph():
    return Neo4jGraph(
        url = st.secrets['neo4j_product_knowledge']["NEO4J_URI"],
        username = st.secrets['neo4j_product_knowledge']["NEO4J_USERNAME"],
        password = st.secrets['neo4j_product_knowledge']["NEO4J_PASSWORD"],
    )


graph = load_knowledge_graph()

@st.cache_resource
def create_vector_space_from_graph():

    vector_index = Neo4jVector.from_existing_graph(
        JinaEmbeddings(model="jina-clip-v2"),
        search_type="hybrid",
        node_label="Document",
        text_node_properties=["text"],
        embedding_node_property="embedding",
        url = st.secrets['neo4j_product_knowledge']["NEO4J_URI"],
        username = st.secrets['neo4j_product_knowledge']["NEO4J_USERNAME"],
        password = st.secrets['neo4j_product_knowledge']["NEO4J_PASSWORD"],
    )

    return vector_index

vector_index = create_vector_space_from_graph()

# @st.cache_resource
# def load_reranker():
#     return JinaRerank(
#         model="jina-reranker-v2-base-multilingual"
#     )

# Create retrival flow
## Extract entities from text
class Entities(BaseModel):
    """Identifying information about entities."""

    names: List[str] = Field(
        ...,
        description="All the person, organization, product, or business entities that "
        "appear in the text",
    )

    @field_validator("names", mode='before')
    def parse_stringified_list(cls, value):
        if isinstance(value, str):
            try:
                # Attempt to parse the string as JSON
                value = json.loads(value)
            except json.JSONDecodeError:
                raise ValueError("Invalid list format; unable to parse string as list.")
        if not isinstance(value, list):
            raise ValueError("items must be a list of strings.")
        return value

prompt = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            "You are extracting organization, product, and person entities from the text.",
        ),
        (
            "human",
            "Use the given format to extract information from the following "
            "input: {question}",
        ),
    ]
)

entity_chain = prompt | llm_groq.with_structured_output(Entities)


# Generate Query
def generate_full_text_query(input: str) -> str:
    full_text_query = ""
    words = [el for el in remove_lucene_chars_cust(input).split() if el]
    for word in words[:-1]:
        full_text_query += f" {word}~2 AND"
    full_text_query += f" {words[-1]}~2"
    return full_text_query.strip()

# Fulltext index query and retirieve context
def structured_retriever(question: str) -> str:
    result = ""
    entities = entity_chain.invoke({"question": question})

    print("="*64)
    print("Question : ", question)
    print("Entities : ", entities)
    print("="*64)
    
    for entity in entities.names:
        response = graph.query(
           """CALL db.index.fulltext.queryNodes('entity', $query, {limit:2})
            YIELD node,score
            CALL(node) {
              MATCH (node)-[r:!MENTIONS]->(neighbor)
              RETURN node.id + ' - ' + type(r) + ' -> ' + neighbor.id AS output
              UNION ALL
              MATCH (node)<-[r:!MENTIONS]-(neighbor)
              RETURN neighbor.id + ' - ' + type(r) + ' -> ' +  node.id AS output
            }
            RETURN output
            """,
            {"query": generate_full_text_query(entity)},
        )
        result += "\n".join([el['output'] for el in response])
    return result

def retrieve_context_by_vector(question):
    question = remove_lucene_chars_cust(question)
    return [el for el in vector_index.similarity_search(question, k=4)]

# def _retriever(): 
#     retriever =  vector_index.as_retriever(
#         search_type = 'similarity',
#         search_kwargs = {
#             'k' : 5,
#         }
#     )

#     return retriever

# compressor = load_reranker()

# Retrival knowledge
def retriever(question: str):
    # print(f"Search query: {question}")
    # retriever = _retriever()

    # compression_retriever = ContextualCompressionRetriever(
    #     base_compressor=compressor, base_retriever=retriever
    # )

    # compressed_docs = compression_retriever.get_relevant_documents(
    #     question
    # )
    structured_data = structured_retriever(question)
    unstructured_data = retrieve_context_by_vector(question)


    documents = []
    
    for doc in unstructured_data:
        sections = ""

        # for key, value in doc.metadata.items() :
        #     sections += f"{key} - {value}\n"
        
        if "Header 1" in doc.metadata:
            sections += f"Header 1 - {doc.metadata['Header 1']}\n"

        if "Header 2" in doc.metadata:
            sections += f"Header 2 - {doc.metadata['Header 2']}\n"

        if "Header 3" in doc.metadata:
            sections += f"Header 3 - {doc.metadata['Header 3']}\n"

        if "Header 4" in doc.metadata:
            sections += f"Header 4 - {doc.metadata['Header 4']}\n"

        if "Header 5" in doc.metadata:
            sections += f"Header 5 - {doc.metadata['Header 5']}\n"

        documents.append(
            f"""
Section :
{sections}
Content :
{doc.page_content.replace("text: ", "")}
"""
        )

    nl = "\n---\n"
    new_line = "\n"
    final_data = f"""

Structured data:
{structured_data}


Unstructured data:
{nl.join(documents)}

"""

    print(final_data)
    return final_data

# Reference:
# {new_line.join(references)}
_template = """
You are an assistant skilled in paraphrasing questions, ensuring they align with the current conversation context. Every time a new question appears, check the recent chat history to decide if it’s on the same topic or if there’s a new topic shift. 

Guidelines:
1. If the latest question is vague (e.g., "What is its capital?"), identify the most recent *explicitly mentioned topic* in the chat history and use it as context.
2. When a new complete question introduces a different topic, assume it’s a topic shift and use this new topic in the next responses until another shift occurs.
3. Prioritize the most recent complete topic if multiple topics are discussed in history.

**Examples:**

Example 1:
**Chat History:**
- User: "Who is the president of Indonesia?"
- AI: "The president of Indonesia is Joko Widodo."

**Latest Question:**  
User: "When did it gain independence?"

**Paraphrased Question:**  
"When did Indonesia gain independence?"

---

Example 2 (Topic Shift):
**Chat History:**
- User: "Who is the president of Indonesia?"
- AI: "The president of Indonesia is Joko Widodo."
- User: "What is its capital?"
- AI: "The capital of Indonesia is Jakarta."
- User: "Who is the president of Vietnam?"
- AI: "The president of Vietnam is Tran Dai Quang."

**Latest Question:**  
User: "What is its capital?"

**Paraphrased Question:**  
"What is the capital of Vietnam?"

---

Example 3:
**Chat History:**
- User: "Who is the CEO of Apple?"
- AI: "The CEO of Apple is Tim Cook."
  
**Latest Question:**  
User: "How many employees does it have?"

**Paraphrased Question:**  
"How many employees does Apple have?"

---

Example 4 (Topic Shift):
**Chat History:**
- User: "Who is the CEO of Apple?"
- AI: "The CEO of Apple is Tim Cook."
- User: "What is the companys revenue?"
- AI: "Apple's revenue is $274.5 billion."

**Latest Question:**  
User: "What is his revenue?"

**Paraphrased Question:**  
"What is the revenue of CEO Apple?"

---

Now, parafrase the latest question based on the recent topic or topic shift, using the latest chat history provided.
But don't explain in  output. just give the parafrased question as output.

**Chat History:**
{chat_history}

**Latest Question:**
{question}

**Paraphrased Question:**
"""

CONDENSE_QUESTION_PROMPT = PromptTemplate.from_template(_template)

# Chat history fromatter
def _format_chat_history(chat_history: List[Tuple[str, str]]) -> List:
    buffer = []
    for human, ai in chat_history:
        buffer.append(HumanMessage(content=human))
        buffer.append(AIMessage(content=ai))
    return buffer

# Extract chat history if exists
_search_query = RunnableBranch(
    # If input includes chat_history, we condense it with the follow-up question
    (
        RunnableLambda(lambda x: bool(x.get("chat_history"))).with_config(
            run_name="HasChatHistoryCheck"
        ),  # Condense follow-up question and chat into a standalone_question
        RunnablePassthrough.assign(
            chat_history=lambda x: _format_chat_history(x["chat_history"])
        )
        | CONDENSE_QUESTION_PROMPT
        | llm_groq
        | StrOutputParser(),
    ),
    # Else, we have no chat history, so just pass through the question
    RunnableLambda(lambda x : x["question"]),
)

# Prompt to real prompt
template = """Your name is El**Ai**s. You are a great, friendly and professional AI chat bot about product from the "Pusat Penelitian Kelapa Sawit Indonesia (PPKS) or Indonesian Oil Palm Research Institute (IOPRI)". The website (https://iopri.co.id/).

### User Question:
{question}

### Retrieved Context:
{context}

### Important Instructions:
- Base your response only on the provided context. Do not assume facts not included here.

Your Answer: """

prompt = ChatPromptTemplate.from_template(template)


# Creating chain for llm
chain = (
    RunnableParallel(
        {
            "context": _search_query | retriever,
            "question": RunnablePassthrough(),
        }
    )
    | prompt
    | llm_groq
    | StrOutputParser()
)

def store_text_area_value():
    st.write(st.session_state['feedback'])


@st.dialog("Berikan Feedback")
def send_feedback():
    with st.form(key="feedback_input", enter_to_submit=False, clear_on_submit=False):
        name = st.text_input("Nama")
        feedback = st.text_area("Feedback")

        rating = [1, 2, 3, 4, 5]
        selected_rating = st.feedback(options="stars")

        # print("INI FEEDBACK: ", feedback)
        if st.form_submit_button("Submit"):
            # Save data to Google Sheets
            if selected_rating is not None:
                save_feedback_to_google_sheets(name, rating[selected_rating], feedback, st.session_state.messages_product_knowledge)
                st.success("Terimakasih atas umpan balik anda!")
            else:
                st.error("Tolong berikan rating 🙏")
            print("INI FEEDBACK: ", feedback)
            # st.write(feedback)

def stream_response(response, delay=0.02):
    for res in response:
        yield res
        time.sleep(delay)

with st.expander("ChatBot PPKS", icon=":material/priority_high:", expanded=True):
    st.markdown(body=
"""
El**Ai**s merupakan asisten virtual dari Pusat Penelitian Kelapa Sawit Indonesia (PPKS) yang dapat memberikan informasi seputar produk dan layanan yang ada di **Product Knowledge : 2023** PPKS.

**Aplikasi** ini sedang dalam pengembangan dan memerlukan **Feedback** dari pengguna.

Silahkan coba untuk menanyakan sesuatu seputar Produk dan Layanan. Setelah itu, mohon untuk mengisi *Feedback Form* dibawah ini
"""
)

    if st.button("Feedback Form", type="primary"):
        send_feedback()


# Displaying all historical messages
for message in st.session_state.messages_product_knowledge:
    st.chat_message(message['role'], avatar= "assets/ELA 1x1.jpg" if message['role'] == "assistant" else None).markdown(message['content'])

if st.session_state.need_greetings_product_knowledge :

    # greet users
    greetings = "Selamat Datang, Saya adalah El**Ai**s, asisten virtual dari Pusat Penelitian Kelapa Sawit Indonesia (PPKS) yang dapat memberikan informasi seputar profil, produk dan layanan yang ada di PPKS, ada yang bisa saya bantu?"
    st.chat_message("assistant", avatar="assets/ELA 1x1.jpg").markdown(greetings)

    st.session_state.messages_product_knowledge.append({'role' : 'assistant', 'content': greetings})

    st.session_state.need_greetings_product_knowledge = False


# Getting chat input from user
prompt = st.chat_input()


# Displaying chat prompt
if prompt:
    # Displaying user chat prompt
    with st.chat_message("user"):
        st.markdown(prompt)

    try :
        # Getting response from llm model
        response = chain.stream({
            "chat_history" : st.session_state.chat_history_product_knowledge, 
            "question" : prompt
        })
        
        # Saving user prompt to session state
        st.session_state.messages_product_knowledge.append({'role' : 'user', 'content': prompt})

        # Displaying response
        with st.chat_message("assistant", avatar="assets/ELA 1x1.jpg"):
            response = st.write_stream(stream_response(response))

        # Saving response to chat history in session state
        st.session_state.messages_product_knowledge.append({'role' : 'assistant', 'content': response})

        # Saving user and llm response to chat history
        st.session_state.chat_history_product_knowledge.append((prompt, response))

        # Just use 3 latest chat to chat history
        if len(st.session_state.chat_history_product_knowledge) > 3:
            st.session_state.chat_history_product_knowledge = st.session_state.chat_history_product_knowledge[-3:]

        # Buat session ID jika belum ada
        if "session_id" not in st.session_state:
            st.session_state.session_id = str(uuid.uuid4())

        # Cetak session ID
        # st.write("Session ID:", st.session_state.session_id)

        end_counter = time.perf_counter()

        total_time = end_counter - start_counter
        st.session_state['total_time'] += total_time 
        # st.write(st.session_state['total_time'], st.session_state['idx_llm'], random_idx)
    except Exception as e:
        st.error("Sedang Terjadi Error, silahkan refresh laman dan coba lagi!")
    
    