# Import the neo4j module
from neo4j import GraphDatabase
import requests
import xml.etree.ElementTree as ET
import os

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

def get_pubmed_info(pmid):
    # Construct the URL for the PubMed API with the given PubMed ID
   
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}&retmode=xml"

    # Send a GET request to the URL and get the response
    response = requests.get(url)

    # Check if the response status code is 200 (OK)
    if response.status_code == 200:
        # Parse the XML content of the response
        root = ET.fromstring(response.content)
        if root.find("PubmedArticle/MedlineCitation/PMID") is None:
            delete_pubmed_node(pmid)
            return None
        
        # require an abstract
        if root.find("PubmedArticle/MedlineCitation/Article/Abstract/AbstractText") is None:
            print(f"No abstract found for PubMed ID: {pmid}")
            delete_pubmed_node(pmid)
            return None

        pmid = root.find("PubmedArticle/MedlineCitation/PMID").text
        url = f"https://www.ncbi.nlm.nih.gov/pubmed/{pmid}"

        # Find the first article element in the XML tree
        article = root.find("PubmedArticle/MedlineCitation/Article")

        # Get the title element text
        title = article.find("ArticleTitle").text

        # Get the Journal element
        journal_title = article.find("Journal/Title").text if article.find("Journal/Title") is not None else ""
        journal_year = article.find("Journal/JournalIssue/PubDate/Year").text if article.find("Journal/JournalIssue/PubDate/Year") is not None else ""
        journal_volume = article.find("Journal/JournalIssue/Volume").text if article.find("Journal/JournalIssue/Volume") is not None else ""
        journal_issue = article.find("Journal/JournalIssue/Issue").text if article.find("Journal/JournalIssue/Issue") is not None else ""

        # Get the list of author elements
        authors = article.findall("AuthorList/Author")

        # Initialize an empty list to store the author names
        author_names = []
    
        # Loop through each author element
        for author in authors:
            # Get the last name and initial elements text
            last_name = author.find("LastName").text if author.find("LastName") is not None else ""
            initial = author.find("Initials").text if author.find("Initials") is not None else ""

            # Append the formatted author name to the list
            author_names.append(f"{last_name} {initial}")

        # Join the author names with commas
        author_names = ", ".join(author_names)

        # Get the abstract element text
        abstract = article.find("Abstract/AbstractText").text

        # Get the list of article id elements
        article_ids = root.findall("PubmedArticle/PubmedData/ArticleIdList/ArticleId")

        # Initialize an empty dictionary to store the article ids by type
        article_ids_dict = {}

        references = root.findall("PubmedArticle/PubmedData/ReferenceList/Reference/ArticleIdList/ArticleId")
        reference_ids = []
        for reference in references:
            reference_type = reference.get("IdType")
            reference_value = reference.text
            if reference_type == "pubmed":
                reference_ids.append(f"{reference_value}")

        reference_ids = ", ".join(reference_ids)

        # Loop through each article id element
        for article_id in article_ids:
            # Get the id type attribute and text value
            id_type = article_id.get("IdType")
            id_value = article_id.text

            # Store the id value in the dictionary with the id type as key
            article_ids_dict[id_type] = id_value

        # Return a tuple of title, author names, abstract, and article ids dictionary
        return (
            title, author_names, abstract, article_ids_dict, journal_title, journal_volume, journal_issue, journal_year,
            reference_ids, pmid, url)

    else:
        # Return None if the response status code is not 200
        return None

# Define a function that takes a node label, an identifier property, an identifier value, and a dictionary of new properties as arguments and updates the node in the database
def update_node(label, id_prop, id_value, props):
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

# Define a function tha will create a Reference node and a relationship to an existing PubMed node
def create_reference_node( reference_id):
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
        create_node("Publication", props)

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
    

# Test the function with an example query that returns all nodes with the label Person
query1 = "MATCH (p:Publication) WHERE p.needs_properties = TRUE RETURN p.pub_id limit 500"

nodes = get_nodes(query1)

for node in nodes:
    print(f"Processing PubMed ID: {node}...")
    info = get_pubmed_info(node)
    if (info is not None):
        label = "Publication"
        id_prop = "pub_id"
        id_value = int(info[9]) if info[9] is not None else 0
        props = {"needs_properties": False, "url": info[10], "title": info[0], "authors": info[1], "abstract": info[2],
                     "journal": info[4], "volume": info[5], "issue": info[6], "year": info[7], "references": info[8]}
        update_node(label, id_prop, id_value, props)
        # validate that the node was updated
        if node_exists("Publication", "pub_id", node):
            print(f"Updated node for PubMed ID: {node}")
        else:
            print(f"Failed to update node for PubMed ID: {node}")
        # create placeholder nodes for references
        if  (info[8] is not None) & needs_references(node) :
            for reference in info[8].split(", "):
                if reference.isdigit() and int(reference) > 0:
                    refId = int(reference)
                    if not node_exists("Publication", "pub_id", refId):
                        create_reference_node(refId)
                    print(f"Created reference node for PubMed ID: {refId}")
                    # add a Reference label to reference node
                    add_label(refId, "Reference")
                    # Create the relationship
                    create_relationship("Publication", "pub_id", id_value, "Publication", "pub_id", refId, "CITES")
        # update the needs_references property
        update_needs_references_property(info[9])
    else:
     print(f"No information found forPubMed ID: {node}")
