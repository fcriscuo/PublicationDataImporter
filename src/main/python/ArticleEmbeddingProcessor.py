import PubMedNeo4jFunctions as pmf
import Llama2_embedding_service as llama2
from multiprocessing import Pool
import time
from langchain.embeddings import OllamaEmbeddings

"""
Python application to generate embeddings for existing PubMed articles and persist the embeddings to the Neo4j database
This application uses the OllamaEmbeddings class from the langchain package to generate the embeddings
A local instance of a llama2 server is required to generate the embeddings
n.b. Github Copilot was used to generate some of the code in this file
Author: Fred Criscuolo
"""


# define a function that will return the abstract property for a specified PubMedArticle node
def get_article_abstract(pubmed_id):
    """
    Return the abstract property for the specified PubMedArticle node
    """
    label = "PubMedArticle"
    id_prop = "pub_id"
    id_value = pubmed_id
    text = pmf.get_node_property(label, id_prop, id_value, "abstract")
    return text


def generate_abstract_embeddings(pubmed_id):
    if pubmed_id is None:
        return
    # get the abstract of the PubMedArticle node
    abstract = get_article_abstract(pubmed_id)
    # check if the abstract is not None or empty
    if abstract != "":
        embedding = llama2.generate_embedding(abstract)
        # set the embedding for the PubMedArticle node
        pmf.set_embeddings_property(pubmed_id, embedding)


# define a function that will generate embeddings for a batch of PubMedArticle nodes
def generate_embeddings_batch(pubmed_ids):
    """
    Generate embeddings for a batch of PubMedArticle nodes
    """
    [pmf.set_embeddings_property(pubmed_id, llama2.generate_embedding(get_article_abstract(pubmed_id))) for pubmed_id in
     pubmed_ids if get_article_abstract(pubmed_id) != ""]


# define a function that will return of pub_ids from PubMedArticle nodes where the embeddings property is null
def get_pubmed_ids_with_null_embeddings(limit=1000):
    """
    Return a list of pub_ids from PubMedArticle nodes where the embeddings property is null
    """
    query = (
                "MATCH (p:PubMedArticle) WHERE p.embeddings IS NULL AND p.abstract IS NOT NULL AND NOT isEmpty(p.abstract) "
                "RETURN p.pub_id limit ") + str(limit)
    pubmed_ids = [pmid for pmid in pmf.get_nodes(query)]
    return pubmed_ids


def main():
    """
    Main function
    Controls the processing of the PubMed data batches
    """
    pmf.create_pubmed_article_id_constraint()
    print("Starting embeddings generation for PubMed articles")
    batch_size = 100
    # Loop until all nodes that have the needs_properties flag set to true have been processed
    while pmf.needs_embeddiings_nodes_exist():
        # Get a batch of nodes that have the needs_properties flag set to true
        pubmed_ids = get_pubmed_ids_with_null_embeddings(batch_size)
        with Pool(processes=4) as pool:
            pool.map(generate_abstract_embeddings, pubmed_ids)


if __name__ == "__main__":
    main()
