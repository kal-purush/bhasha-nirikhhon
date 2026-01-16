from dotenv import load_dotenv
import os
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langchain.text_splitter import CharacterTextSplitter
from langchain_community.document_loaders import TextLoader
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings


load_dotenv()

model = ChatOpenAI(model = "gpt-4o-mini")
model2 = ChatOpenAI(model = "gpt-4o-mini")

chat_history = []  # Use a list to store messages
answer_history = [] # Stores the conversation between the user and the AI.

# Set an initial system message (optional)
chat_history.append(SystemMessage(content="You are an AI assistant that will help me classify the emotion conveyed in the user's sentence."))  # Add system message to chat history
chat_history.append(SystemMessage(content="The classification must fall within one of the following categories: [joy, sadness, anger, fear, disgust, embarrassment, anxiety, nostalgia, envy, boredom]."))
chat_history.append(SystemMessage(content="Your responses should just state the emotion. e.g. 'Joy' or 'Anger'"))

answer_history.append(SystemMessage(content = "Your responses should just reflect what the character says, no other information is needed. Think of it as having a conversation."))

# Define the persistent directory
current_dir = os.path.dirname(os.path.abspath(__file__))
persistent_directory = os.path.join(current_dir, "db", "chroma_db")

# Define the embedding model
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

# Load the existing vector store with the embedding function
db = Chroma(persist_directory=persistent_directory,
            embedding_function=embeddings)

# Chat loop
while True:
    query = input("You: ")
    if query.lower() == "exit":
        break
    chat_history.append(HumanMessage(content=query))  # Add user message

    # Get AI response using history
    result = model.invoke(chat_history)
    response = query + " " + result.content
    chat_history.append(AIMessage(content=response))  # Add AI message

    # Retrieve relevant documents based on the query
    retriever = db.as_retriever(
        search_type="similarity_score_threshold",
        search_kwargs={"k": 3, "score_threshold": 0.1},
    )
    relevant_docs = retriever.invoke(response)

    combined_input = (
    "Here are some documents that might help answer the question: "
    + f"How would {result.content} respond to the user's query, {query} if they were having a conversation?"
    + "\n\nRelevant Documents:\n"
    + "\n\n".join([doc.page_content for doc in relevant_docs])
    + "\n\nUnderstand how that character responds semantically and provide a reply to the user's query."
)

    answer_history.append(HumanMessage(content=combined_input))  # Add AI message
    reply = model2.invoke(answer_history)

    print(f"AI: {reply.content}")


# print("---- Message History ----")
# print(chat_history)

# Invoke the model with a message
# result = model.invoke("What is 81 divided by 9?")
# print("Full result:")
# print(result)
# print("Content only:")
# print(result.content)