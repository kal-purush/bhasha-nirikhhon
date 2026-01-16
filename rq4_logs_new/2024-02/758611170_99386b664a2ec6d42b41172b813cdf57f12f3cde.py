import streamlit as st
import openai

from llama_index import (
    SimpleDirectoryReader,
    ServiceContext,
    OpenAIEmbedding,
    PromptHelper,
    VectorStoreIndex,
    set_global_service_context,
    Document,
)
from llama_index.llms import OpenAI
from llama_index.text_splitter import SentenceSplitter

# from pinecone import Pinecone
# from llama_index.vector_stores import PineconeVectorStore
from llama_index.storage.storage_context import StorageContext


st.set_page_config(page_title="Theo The Thesis Chatbot", 
                   page_icon=":book:", 
                   layout="centered", 
                   initial_sidebar_state="auto",
                   menu_items=None)

st.markdown("""
<style>
.big-font {
    font-size:20px !important;
}
</style>
""", unsafe_allow_html=True)



st.title("Hi, I'm Theo the Thesis Chatbot!")
# st.write("By Adam Goodkind, for his thesis [*Predicting Social Dynamics in Interactions Using Keystroke Patterns*](https://adamgoodkind.com/files/Goodkind_Dissertation.pdf)")
# st.header("By Adam Goodkind, for his thesis [*Predicting Social Dynamics in Interactions Using Keystroke Patterns*](https://adamgoodkind.com/files/Goodkind_Dissertation.pdf)")
st.markdown('<p class="big-font">By Adam Goodkind <br> For my thesis \
            <a href="https://adamgoodkind.com/files/Goodkind_Dissertation.pdf"><em>Predicting Social Dynamics in Interactions Using Keystroke Patterns</em></a></p>', unsafe_allow_html=True)

# access key from .streamlit/secrets.toml
openai.api_key = st.secrets.openai_key

# initiate a Pinecone client
# pinecone_client = Pinecone(api_key=st.secrets.PINECONE_API_KEY)
# pinecone_index = pinecone_client.Index("theo")


if "messages" not in st.session_state.keys(): # Initialize the chat messages history
    st.session_state.messages = [
        {"role": "assistant", "content": "Ask me a question about Adam's thesis!"}
    ]

@st.cache_resource(show_spinner=False)
def load_data():
    with st.spinner(text="I'm reading all 227 pages of Adam's thesis – hang tight! This might take a moment."):
        reader = SimpleDirectoryReader(input_dir="./data", recursive=True)
        docs = reader.load_data()
        
        # parameters for the Service Context
        llm = OpenAI(model="gpt-4", 
                     temperature=st.session_state.llm_temp, 
                     max_tokens=256,
                     top_p=st.session_state.llm_top_p,
                     system_prompt="""
                        You are a smart and educated person, 
                        and your job is to answer questions about Adam's thesis. 
                        Assume that all questions are related to Adam's entire thesis. 
                        But if the thesis doesn't have the answer, try looking to
                        your general knowledge.
                        But make sure your answers are complete but also concise.
                        """)
        embed_model = OpenAIEmbedding(embed_batch_size=25)
        text_splitter = SentenceSplitter(chunk_size=1024, chunk_overlap=20)
        prompt_helper = PromptHelper(
            context_window=4096,
            num_output=256,
            chunk_overlap_ratio=0.1,
            chunk_size_limit=None,
        )

        # the Service Context is a bundle used for indexing and querying
        service_context = ServiceContext.from_defaults(
            llm=llm,
            embed_model=embed_model,
            text_splitter=text_splitter,
            prompt_helper=prompt_helper,
        )

        # # Set the global service context to the cre  
        
        index = VectorStoreIndex.from_documents(docs, 
                                                service_context=service_context, 
                                                show_progress=True)

        # # Create a PineconeVectorStore using the specified pinecone_index
        # vector_store = PineconeVectorStore(pinecone_index=pinecone_index)

        # # Create a StorageContext using the created PineconeVectorStore
        # storage_context = StorageContext.from_defaults(vector_store=vector_store)

        # # Use the chunks of documents and the storage_context to create the index
        # index = VectorStoreIndex.from_documents(
        #     docs, 
        #     storage_context=storage_context
        #     )
        
        return index

def print_llm_state():
    print("llm_temp: {}".format(st.session_state.llm_temp))
    print("llm_top_p: {}".format(st.session_state.llm_top_p))

st.markdown(
    """
    <style>
        [data-testid=stSidebar] [data-testid=stImage]{
            text-align: center;
            display: block;
            margin-left: auto;
            margin-right: auto;
            width: 100%;
        }
    </style>
    """, unsafe_allow_html=True
)

with st.sidebar:
    # st.markdown(" <style> div[class^='st-emotion-cache-6qob1r'] { padding-top: 0rem; } </style> ", unsafe_allow_html=True)    
    st.sidebar.image("./images/theo_icon.jpeg")
    st.title("How creative do you want me to be in my answers?")
    # st.write("Have fun with the settings below. [OpenAI](https://platform.openai.com/docs/api-reference/chat/create) \
    #          recommends only adjusting one setting at a time (usually temperature)/")

    llm_temperature = st.slider(label = "**Temperature**: Randomness of my word choices, \
                                from predictable (0) to imaginative (1)", 
                                key="llm_temp",
                                min_value=0.0, max_value=1.0, step=.05, value = 0.9,
                                on_change = print_llm_state)
    
    lmm_top_p = st.slider(label = "**Nucleus Sampling**: The size (proportion) of \
                                the word pool to sample from", key="llm_top_p",
                                min_value=0.0, max_value=1.0, step=.05, value = 1.0,
                                on_change = print_llm_state)
    # st.caption("It's usually best to keep this at 1.0")

index = load_data()

if "chat_engine" not in st.session_state.keys(): # Initialize the chat engine
        st.session_state.chat_engine = index.as_chat_engine(
            chat_mode="condense_question", 
            verbose=True)

if prompt := st.chat_input("Your question"): # Prompt for user input and save to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

for message in st.session_state.messages: # Display the prior chat messages
    with st.chat_message(message["role"]):
        st.write(message["content"])

# If last message is not from assistant, generate a new response
if st.session_state.messages[-1]["role"] != "assistant":
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            response = st.session_state.chat_engine.chat(prompt)
            st.write(response.response)
            message = {"role": "assistant", "content": response.response}
            st.session_state.messages.append(message) # Add response to message history