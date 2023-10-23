import PubMedNeo4jFunctions as pmf
import PubMedXMLFunctions as pxf

"""
Python application to import PubMed data into a Neo4j database
The application queries the Neo4j database for placeholder Publication nodes (i.e. nodes that have the needs_properties flag set to true)
The application then uses the E-utilities API to fetch the PubMed data from NCBI and persists the data to the database
n.b. Github Copilot was used to generate some of the code in this file
Author: Fred Criscuolo
"""

# Define a function that will query the database for n nodes and return the result as String of comma separated integers

def get_needs_properties_batch(n):
    """
    Query the database for n nodes that have the needs_properties flag set to true
    Return the result as a String of comma separated integers
    Input: n - the number of nodes to return
    """
    # Define the query
    query = "MATCH (p:Publication) WHERE p.needs_properties = TRUE RETURN p.pub_id limit " + str(n)
    # Return the list of node IDs as a list of integers
    pubmed_ids = [int(pmid) for pmid in pmf.get_nodes(query)]
    return pubmed_ids

# Define a function that will persist the data to the database


def persist_pubmed_data(pubmed_data):
    """
    Persist the data to the Neo4j database
    """
    label = "Publication"
    id_prop = "pub_id"
    id_value = int(pubmed_data["pmid"]
                   ) if pubmed_data["pmid"] is not None else 0
    # print(f"Updating node {label} {id_prop} {id_value}")
    pmf.update_node(label, id_prop, id_value, pubmed_data)

# Define a main function


def main():
    """
    Main function
    Controls the processing of the PubMed data batches
    """
    print("Starting PubMed data import")
    batch_size = 100
    # Loop until all nodes that have the needs_properties flag set to true have been processed
    while pmf.needs_properties:
        # Get a batch of nodes that have the needs_properties flag set to true
        pubmed_ids = get_needs_properties_batch(batch_size)
        # print(f"Processing batch of {batch_size} nodes: {pubmed_ids}")
        pubmed_articles = pxf.get_pubmed_articles(pubmed_ids)
        if pubmed_articles is None or not pubmed_articles:
            print("No articles returned for pubmed_ids: {pubmed_ids}")
            for pubmed_id in pubmed_ids:
                pmf.turn_off_properties_flags(pubmed_id)
        else:
            for pubmed_article in pubmed_articles:
                data = pxf.extract_pubmed_data(pubmed_article)
                # print(f"{data}\n")
                print(f"Processing PMID {data['pmid']}")
                persist_pubmed_data(data)

    print("Finished PubMed data import")


if __name__ == "__main__":
    main()
