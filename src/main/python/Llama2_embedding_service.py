import sys
import time
import threading
from langchain.embeddings import OllamaEmbeddings

"""
Python script to generate llama2 embeddings for a supplied text
This application uses the OllamaEmbeddings class from the langchain package to generate the embeddings
A local instance of a llama2 server is required to generate the embeddings
n.b. Github Copilot was used to generate some of the code in this file
Author: Fred Criscuolo
"""
embeddings = OllamaEmbeddings(model="llama2")
print("A connection to a local llama2 server has been established")


# define a function that will use embeddings to generate an embedding for a given text
def generate_embedding(text):
    """
    Generate an embedding for the given text
    """
    start_time = time.time()
    embedding = embeddings.embed_query(text)
    duration= time.time() - start_time
    print(f"Generating embedding for {len(text)} characters required {duration:.2f} seconds")
    return embedding

def main():
    """
    Main function
    """
    # get the text from the command line
    text = sys.argv[1] if len(sys.argv) > 1 else "The quality of mercy is not strained."
    # generate the embedding
    embedding = generate_embedding(text)
    # print the embedding
    print(embedding[:6])


if __name__ == "__main__":
    main()