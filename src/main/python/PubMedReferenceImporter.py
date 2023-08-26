# import the neo4j python driver
from neo4j import GraphDatabase
import os

# create a driver object to connect to the database
uri = "neo4j://localhost:7687"
user = os.environ.get('NEO4J_USER', "neo4j")
password = os.environ.get('NEO4J_PASSWORD', "neo4j")
driver = GraphDatabase.driver(uri, auth=(user, password))
print(f"Neo4j Driver created: {driver}")
# define a function to process a reference identifier


def process_reference(pubmed_id, reference_id):
    print(
        f"Processing reference {reference_id} for PubMed article {pubmed_id}")
    # create a session object
    with driver.session() as session:
        # define a cypher query to check if the reference identifier already exists as a Publication node
        check_query = """
        MATCH (p:Publication)
        WHERE p.pub_id = $reference_id
        RETURN p
        """
        # run the query and get the result object
        result = session.run(check_query, reference_id=reference_id)
        # get the first record from the result object
        record = result.single()
        # if the record is not None, it means the reference identifier already exists as a Publication node
        if record is not None:
            # get the node from the record
            node = record["p"]
            # check if the node already has the Reference label
            if "Reference" in node.labels:
                # do nothing
                pass
            else:
                # add the Reference label to the node
                add_label_query = """
                MATCH (p:Publication)
                WHERE p.pub_id = $reference_id
                SET p:Reference
                """
                try:
                    session.run(add_label_query, reference_id=reference_id)
                except Exception as e:
                    print(
                        f"Exception adding label to node {reference_id}: {e}")
                else:
                    # print a message that the label was added
                    print(f"Reference label added to node {reference_id}")
        else:
            # create a new node with the labels Publication and Reference and the properties pub_id, need_properties, and needs_references
            create_node_query = """
            CREATE (r:Publication:Reference {pub_id: $reference_id, need_properties: true, needs_references: false})
            """
            session.run(create_node_query, reference_id=reference_id)
            # print a message that the node was created
            print(f"Reference {reference_id} was created")

        # create a relationship of type CITES between the PubMed node and the Reference node
        create_rel_query = """
        MATCH (p:Publication {pub_id: $pubmed_id})
        MATCH (r:Publication {pub_id: $reference_id})
        MERGE (p)-[:CITES]->(r)
        """
        session.run(create_rel_query, pubmed_id=pubmed_id,
                    reference_id=reference_id)


def persist_reference_data(pubmed_id, reference_ids):
    for reference_id in reference_ids:
        # print(f"PubMedReferenceImporter: processing reference {reference_id} for PubMed article {pubmed_id}")
        process_reference(pubmed_id, reference_id)

# define a main function


def main():
    # get the input values from the user or another source
    pubmed_id = 19247474  # example value, change as needed
    reference_ids = [10451441, 12740294, 16539894, 15697051, 15500307,
                     15170444, 16374522, 10597412, 17888884, 16671072,
                     17579606, 16960812, 15048644, 14975165, 17436240,
                     15211640, 14679582, 16472381, 17763112, 14557728,
                     17179996, 15192278, 18205015, 11280926, 8774539,
                     11105655, 15007373, 17978999, 10822347, 9521442,
                     18188666, 7550364, 10975602, 9641486, 17112802,
                     11303596, 15308589, 16176798, 15735609, 17454707,
                     15790597, 16874522, 18385739, 18227835, 18385676,
                     18385738, 18385720, 18618000, 17135278, 17529973,
                     17401363, 10364535, 16862161, 11189684, 16222627,
                     12111919, 11793788, 17158188, 14639705, 17966091,
                     16936733, 12492752, 18460664, 17855705, 18190672,
                     12564384, 16157522, 12045467, 16763378, 9596006,
                     12902625, 12815741, 12563176, 16207390, 16859748,
                     17885625, 17237345, 17081982, 15653710, 17979512,
                     18041664, 16041240, 11805739, 15520277, 17898222,
                     17372231, 17179752, 15266341]  # example list, change as needed

    persist_reference_data(pubmed_id, reference_ids)
    # loop through each reference identifier and process it
    for reference_id in reference_ids:
        process_reference(pubmed_id, reference_id)


# call the main function if the script is run as the main program
if __name__ == "__main__":
    main()

# close the driver object
driver.close()
