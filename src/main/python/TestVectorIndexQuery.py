import os

from neo4j import GraphDatabase
from langchain.vectorstores.neo4j_vector import Neo4jVector
from langchain.embeddings import OllamaEmbeddings
from langchain.text_splitter import CharacterTextSplitter
from langchain.docstore.document import Document

"""
A collection of Python functions to interact with a Neo4j database
n.b. Code development assisted by Github Copilot
Author: Fred Criscuolo
"""

# Define the database URI, user, and password
# User and password are set as environment variables with standard Neo4j defaults
uri = "neo4j://localhost:7687"
user = os.environ.get('NEO4J_USER', "neo4j")
dimension = 4096
password = os.environ.get('NEO4J_PASSWORD', "neo4j")
embeddings = OllamaEmbeddings(model="llama2")

# Create a driver object to connect to the database
driver = GraphDatabase.driver(uri, auth=(user, password))

with driver.session() as session:
        # Construct a query that matches the node by label and identifier property and value, and returns the value of the property
        query = f"match (p:PubMedArticle) where p.embeddings is not null return count(p) "
        # Run the query with the parameters
        result = session.run(query)
        # Return the property value
        record = result.single()
        print(f"Record count = {record}")

existing_index = Neo4jVector.from_existing_index(
    embedding=embeddings,
    url=uri,
    username=user,
    password=password,
    index_name="pubmedarticle",
    text_node_property="abstract",  # Need to define if it is not default
)

print(existing_index.node_label)
print(existing_index.embedding_node_property)