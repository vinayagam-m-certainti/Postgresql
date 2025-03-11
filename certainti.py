import os
import nest_asyncio
import streamlit as st
from llama_index.core import SimpleDirectoryReader, Settings, VectorStoreIndex
from llama_index.llms.ollama import Ollama
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.vector_stores.chroma import ChromaVectorStore
import chromadb

nest_asyncio.apply()

# Suppress Hugging Face symlink warning
os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"

# Streamlit UI
st.set_page_config(page_title="PDF Summarizer Chatbot (Ollama - Mistral)", layout="wide")
st.title("📄 PDF Summarizer & Quantifier Chatbot (Ollama - Mistral)")

# Initialize ChromaDB client and collection outside the upload block for persistence
chroma_client = chromadb.PersistentClient(path="./chroma_db")
chroma_collection = chroma_client.get_or_create_collection(name="pdf_vectors")
vector_store = ChromaVectorStore(chroma_collection=chroma_collection) # Pass the collection object

# Configure LlamaIndex settings - Initialize them outside the conditional block
Settings.llm = Ollama(model="mistral", request_timeout=600.0, host="127.0.0.1:11435")
Settings.embed_model = HuggingFaceEmbedding()


# Function to process and index PDF
def process_pdf(uploaded_file):
    if uploaded_file is not None:
        # Save the uploaded file temporarily
        with open("temp.pdf", "wb") as f:
            f.write(uploaded_file.read())

        # Load the document from the temporary file
        documents = SimpleDirectoryReader(input_files=["temp.pdf"]).load_data()

        # **🔹 Step 4: Create and Store Vector Index (or append to existing)**
        # We are now creating a new index from ALL documents in the vector store.
        # This will include previously added documents.
        vector_index = VectorStoreIndex.from_documents(documents, vector_store=vector_store, rebuild_index=False) # added rebuild_index=False

        # Query engine setup
        vector_query_engine = vector_index.as_query_engine()
        return vector_query_engine
    return None


# Upload PDF file
uploaded_file = st.file_uploader("Upload a PDF file", type=["pdf"])

vector_query_engine = process_pdf(uploaded_file) # Process PDF only when a new file is uploaded, and get the query engine

if vector_query_engine: # Only proceed if a PDF has been processed and query engine is available

    # Display summary
    st.subheader("📜 Summary")
    response = vector_query_engine.query("Summarize the given document and also summarise all the previous documents knowledge if any.") # Modified summary prompt
    st.write(response.response)

    # Chat interface
    st.subheader("💬 Ask about the document")
    user_input = st.text_input("Type your question here...")

    if user_input:
        answer = vector_query_engine.query(user_input)
        st.write(f"🤖 MistralBot: {answer.response}")

    # Clean up temporary file
    os.remove("temp.pdf")