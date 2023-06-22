from neo4j import GraphDatabase
import requests
import xml.etree.ElementTree as ET
import os
import PubMedReferenceImporter as pri   

# Define the database URI, user, and password
uri = "neo4j://tosca.local:7687"
user = os.environ.get('NEO4J_USER',"neo4j")
password = os.environ.get('NEO4J_PASSWORD',"neo4j")

# Create a driver object to connect to the database
driver = GraphDatabase.driver(uri, auth=(user, password))

# Define a function that takes a query as an argument and returns a list of nodes
def get_nodes(query):
    # Initialize an empty list to store the nodes
    nodes = []
    # Use a context manager to create a session with the driver
    with driver.session() as session:
        # Run the query and get the result
        result = session.run(query)

        # Loop through each record in the result
        for record in result:
            # Get the first value in the record, which is a node object
            node = record.values()[0]

            # Append the node to the list
            nodes.append(node)

    # Return the list of nodes
    return nodes

# Define a function that takes a node label, an identifier property, an identifier value, and a dictionary of new properties as arguments and updates the node in the database
def update_node(label, id_prop, id_value, props):
    #print(f"PubMedNeo4jFunctions: update_node {label} {id_prop} {id_value}")
    # confirm that the node exists
    if not node_exists(label, id_prop, id_value):    
        print(f"Node for {label} {id_prop} {id_value} does not exist.")
        return
    # Use a context manager to create a session with the driver
    with driver.session() as session:
        # Construct a query that matches the node by label and identifier property and value, and sets the new properties
        query = f"MATCH (n:{label}) WHERE n.{id_prop} = {id_value} SET n += $props"
        # Run the query with the parameters
        session.run(query, id_value=id_value, props=props)
        session.close()
        references = props["references"]
        #print(f"procsessing references {references}")
        if references is not None:
            create_reference_nodes(id_value, references)
            #print(f"processing references for {id_value} {references}")
            pri.persist_reference_data(id_value, references)    

#Define a function that will create placholder nodes for the references
def create_reference_nodes(pub_id, reference_ids):
    # Loop through each reference id
    for reference_id in reference_ids:
        # Create the reference node
        create_reference_node(pub_id, reference_id)

# Define a function tha will create a Reference node and a relationship to an existing PubMed node
def create_reference_node( pub_id,reference_id):
    # Initialize an empty dictionary to store the properties
    props = {}
    
    # Set the reference ID property
    props["pub_id"] = reference_id

    # Set the needs_properties property
    props["needs_properties"] = True

    # Set the needs_references property
    props["needs_references"] = False

    # Create the Reference node
    if not node_exists("Publication", "pub_id", reference_id):
        print(f"Creating Reference node for {reference_id}")
        create_node("Publication", props)
    add_label(reference_id, "Reference")
    # Create the relationship
    create_relationship("Publication", "pub_id", pub_id, "Publication", "pub_id", reference_id, "CITES")
    update_needs_references_property(pub_id)

# Define a function to determine if a node exists in the database
# default id_prop is pub_id
#default labe is Publication
#default id_value is 0
def node_exists(label="Publication", id_prop="pub_id", id_value = 0):  
    # Use a context manager to create a session with the driver
    with driver.session() as session:
        # Construct a query that matches the node by label and identifier property and value
        query = f"MATCH (n:{label}) WHERE n.{id_prop} = {id_value} RETURN n"
        # Run the query with the parameters
        result = session.run(query, id_value=id_value)
        # Return True if the result contains a record
        return result.single() is not None
    
# Define a function to determine if a Publication node's needs_references is FALSE
def needs_references(pmid):
    # Use a context manager to create a session with the driver
    with driver.session() as session:
        # Construct a query that matches the node by label and identifier property and value
        if node_exists("Publication", "pub_id", pmid):
            query = f"MATCH (n:Publication) WHERE n.pub_id = {pmid} RETURN n.needs_references as exists"    
            # Run the query with the parameters
            result = session.run(query, id_value=pmid)
            record = result.single()
            return record is not None and record["exists"] is True
        else:
            return False
        
def create_relationship(label1, id_prop1, id_value1, label2, id_prop2, id_value2, rel_type):
    # Use a context manager to create a session with the driver
    with driver.session() as session:
        # Construct a query that matches the nodes by label and identifier property and value, and creates a relationship between them
        query = f"MATCH (n1:{label1} {{{id_prop1}: {id_value1}}}), (n2:{label2} {{{id_prop2}: {id_value2}}}) MERGE (n1)-[:{rel_type}]->(n2)"
        # Run the query with the parameters
        session.run(query, id_value1=id_value1, id_value2=id_value2)

def create_node(label, props):
    # Use a context manager to create a session with the driver
    with driver.session() as session:
        # Construct a query that creates a node with the given label and properties
        query = f"CREATE (n:{label} $props)"
        # Run the query with the parameter
        session.run(query, props=props)

def node_has_label(node_id, label):
    # Use a context manager to create a session with the driver
    with driver.session() as session:
        # Construct a query that matches the Publication node and node_id and adds the label
        query = f"MATCH (n:Publication) WHERE n.pub_id = {int(node_id)} AND n:{label} RETURN TRUE AS exists"
        # Run the query with the parameter
        result = session.run(query, node_id=node_id)
        # Return True if the result contains a record  
        record = result.single()
        exists = bool(record["exists"]) if record is not None else False
        return exists

# Define a function that takes a node id and a label as arguments and adds the label to the node in the database
def add_label(node_id, label):
    # Use a context manager to create a session with the driver
    with driver.session() as session:
        # determine if the node already has this label
        if node_has_label(node_id, label):
            print(f"Node {node_id} already has label {label}.")
            return
        # Construct a query that matches the Publication node and node_id and adds the label
        query = f"MATCH (n:Publication) WHERE n.pub_id = {int(node_id)} SET n:{label}"
        # Run the query with the parameter
        session.run(query, node_id=node_id)

# Define a function to detach and delete a Publication node based on its PubMed ID
def delete_pubmed_node(pmid):
    # Use a context manager to create a session with the driver
    with driver.session() as session:
        # Construct a query that detaches and deletes the node by label and identifier property and value
        query = f"MATCH (n:Publication) WHERE n.pub_id = {pmid} DETACH DELETE n"

        # Run the query with the parameters
        session.run(query, id_value=pmid)
        print(f"Deleted PubMed ID: {pmid}")

def update_needs_references_property(pmid):
    # Use a context manager to create a session with the driver
    with driver.session() as session:
        # Construct a query that matches the node by label and identifier property and value, and sets the new properties
        query = f"MATCH (n:Publication) WHERE n.pub_id = {pmid} SET n.needs_references = FALSE"
        # Run the query with the parameters
        session.run(query, id_value=pmid)
        session.close()
    
     # Define a function that will determine if there are any nodes in the database that have the needs_properties property set to TRUE
def needs_properties():
    # Use a context manager to create a session with the driver
    with driver.session() as session:
        # Construct a query that matches the node by label and identifier property and value, and sets the new properties
        query = "MATCH (n:Publication) WHERE n.needs_properties = TRUE RETURN n.pub_id as pub_id"
        # Run the query with the parameters
        result = session.run(query)
        # Return True if the result contains a record
        return result.single() is not None

