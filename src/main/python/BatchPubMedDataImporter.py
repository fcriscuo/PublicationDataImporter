
from lxml import etree as ET
import PubMedNe04jFunctions as pmf
import PuvMedXMLFunctions as pxf

# Define a function that will query the database for n nodes and return the result as String of comma separated integers
def get_needs_properties_batch(n):
    # Define the query
    query = "MATCH (p:Publication) WHERE p.needs_properties = TRUE RETURN p.pub_id limit " + str(n)

    # Get the list of nodes
    nodes = pmf.get_nodes(query)

    # Initialize an empty list to store the node IDs
    node_ids = []

    # Loop through each node
    for node in nodes:
        print(node)
        # Add the pub_id tot the list
        node_ids.append(str(node))

    # Return the list of node IDs as a comma separated string
    return ",".join(node_ids)

# Define a function that will parse and print the ArticleTitle from an PubmedArticle XML element
def parse_pubmed_article(pubmed_article):
    # extract the title from each article
    title = pubmed_article.find("MedlineCitation/Article/ArticleTitle").text
    print(f"Title: {title}")

#Define a function that will persist the data to the database
def persist_pubmed_data(pubmed_data):
    label = "Publication"
    id_prop = "pub_id"

    id_value = int(pubmed_data["pmid"]) if pubmed_data["pmid"] is not None else 0
    props = {"needs_properties": False, "url":pubmed_data["url"], "title": pubmed_data["title"], "authors": pubmed_data["authors"], "abstract": pubmed_data["abstract"],
                     "journal": pubmed_data["journal"], "volume": pubmed_data["volume"], "issue": pubmed_data["issue"], "year": pubmed_data["year"], "references":pubmed_data["references"],
                     "pmc_id": pubmed_data["pmc_id"], "pmc_url": pubmed_data["pmc_url"], "doi": pubmed_data["doi"]}
    pmf.update_node(label, id_prop, id_value, props)

#Define a main function
def main():
    print("Starting PubMed data import")
    batch_size = 200
    # Loop until all nodes that have the needs_properties flag set to true have been processed
    while pmf.needs_properties:     
        pubmed_ids = get_needs_properties_batch(batch_size)
        pubmed_articles = pxf.get_pubmed_articles(pubmed_ids)
        for pubmed_article in pubmed_articles:
            data = pxf.extract_pubmed_data(pubmed_article)
            persist_pubmed_data(data)
    print("Finished PubMed data import")

if __name__ == "__main__":
    main()