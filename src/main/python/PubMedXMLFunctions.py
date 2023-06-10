import requests
import xml.etree.ElementTree as ET

# Define a function that takes in a List of pubmed ids and returns a list of PubmedArticle XML elements
def get_pubmed_articles(pubmed_ids):
    # Convert the list of pubmed ids to a comma separated string
    pubmed_ids = ",".join([str(pmid) for pmid in pubmed_ids])
    # Fetch the data from NCBI
    response = fetch_pubmed_data(pubmed_ids)
    # Parse the XML response
    pubmed_articles = parse_pubmed_articles_from_xml(response)
    # Return the list of PubmedArticle XML elements
    return pubmed_articles

#Define a function that will fetch a batch of PubMed data from NCBI using the E-utilities API
def fetch_pubmed_data(pubmed_ids):
    # Define the base URL for the E-utilities API
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pubmed_ids}&retmode=xml"
 
    response = requests.get(url)
    # Return the response
    return response

# Define a function to parse the XML response from NCBI
def parse_pubmed_articles_from_xml(response):
    # Check if the response status code is 200 (OK)
    if response.status_code == 200:
        # Parse the XML content of the response
        root = ET.fromstring(response.content)

        pubmed_articles = root.findall("PubmedArticle")
        return pubmed_articles
    else:
        # Print the status code
        print(response.status_code)
        return None
    
# Define a function that will parse the requisite data from the PubmedArticle XML element
def extract_pubmed_data(pubmed_article):
    # extract the PMD ID from each article
    pmid = int( pubmed_article.find("MedlineCitation/PMID").
                text) if pubmed_article.find("MedlineCitation/PMID") is not None else 0
    url = f"https://www.ncbi.nlm.nih.gov/pubmed/{pmid}"
    # extract the title from each article
    title = pubmed_article.find("MedlineCitation/Article/ArticleTitle").text
    #extract the abstract from each article
    abstract = pubmed_article.find("MedlineCitation/Article/Abstract/AbstractText").text if pubmed_article.find("MedlineCitation/Article/Abstract/AbstractText") is not None else ""
    # extract the journal title from each article
    journal = pubmed_article.find("MedlineCitation/Article/Journal/Title").text if pubmed_article.find("MedlineCitation/Article/Journal/Title") is not None else ""
    # extract the volume from each article
    volume = pubmed_article.find("MedlineCitation/Article/Journal/JournalIssue/Volume").text if pubmed_article.find("MedlineCitation/Article/Journal/JournalIssue/Volume") is not None else ""
    # extract the issue from each article
    issue = pubmed_article.find("MedlineCitation/Article/Journal/JournalIssue/Issue").text if pubmed_article.find("MedlineCitation/Article/Journal/JournalIssue/Issue") is not None else ""
    # extract the year from each article
    year = pubmed_article.find("MedlineCitation/Article/Journal/JournalIssue/PubDate/Year").text if pubmed_article.find("MedlineCitation/Article/Journal/JournalIssue/PubDate/Year") is not None else ""
    # extract the author names from each article
    authors = extract_author_names(pubmed_article)
    # extract the article IDs from each article
    pmc_id, doi = extract_article_ids(pubmed_article)
    pmc_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc_id}/" if pmc_id != "" else ""
    # extract the reference IDs from each article
    references = extract_reference_ids(pubmed_article)
    # Return the data as a dictionary
    return {"pmid": pmid, "url": url, "title": title, "abstract": abstract, "journal": journal,
             "volume": volume, "issue": issue, "year": year, "authors": authors, "pmc_id": pmc_id, 
             "pmc_url": pmc_url, "doi": doi, "references": references}


#Define a function to parse the reference ids from the article as a joined String
def extract_reference_ids(pubmed_article):
    # Get the list of reference elements
    references = pubmed_article.findall("PubmedData/ReferenceList/Reference")

    # Initialize an empty list to store the reference ids
    reference_ids = []

    # Loop through each reference element
    for reference in references:
        # Get the PMID element text
        pmid = reference.find("ArticleIdList/ArticleId[@IdType='pubmed']").text

        # Append the pmid to the list
        reference_ids.append(pmid)

    # Join the reference ids with commas
    reference_ids = ", ".join(reference_ids)

    # Return the reference ids
    return reference_ids

# Define a function that will parse the pmid idtype and doi idType from the articleIds XML element
def extract_article_ids(pubmed_article):
    # Get the list of articleId elements
    article_ids = pubmed_article.findall("PubmedData/ArticleIdList/ArticleId")

    # Initialize empty variables to store the pmid and doi
    pmc_id = ""
    doi = ""

    # Loop through each articleId element
    for article_id in article_ids:
        # Get the IdType attribute value
        id_type = article_id.attrib["IdType"]

        # Check if the IdType is equal to "pmc"
        if id_type == "pmc":
            # Get the text of the articleId element
            pmc_id = article_id.text
        # Check if the IdType is equal to "doi"
        elif id_type == "doi":
            # Get the text of the articleId element
            doi = article_id.text

    # Return the pmid and doi
    return pmc_id, doi

# Define a function that will extraxt author names
def extract_author_names(pubmed_article):
    # Get the list of author elements
    authors = pubmed_article.findall("MedlineCitation/Article/AuthorList/Author")
    # Initialize an empty list to store the author names
    author_names = []
    
    # Loop through each author element
    for author in authors:
        print(f"Author: {author}")
        # Get the last name and initial elements text
        last_name = author.find("LastName").text if author.find("LastName") is not None else ""
        initial = author.find("Initials").text if author.find("Initials") is not None else ""

        # Append the formatted author name to the list
        author_names.append(f"{last_name} {initial}")

    # Join the author names with commas
    author_names = ", ".join(author_names)

    # Return the author names
    return author_names

# Define a main method for testing
def main():
    pubmed_ids = [17523278, 18581211, 18704002, 8084603, 19681044]
    print(pubmed_ids)
    pubmed_articles = get_pubmed_articles(pubmed_ids)
    for pubmed_article in pubmed_articles:
        data = extract_pubmed_data(pubmed_article)
        print(f"{data}\n")

# Invoke the main method
if __name__ == "__main__":
    main()
