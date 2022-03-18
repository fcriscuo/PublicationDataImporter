package org.batteryparkdev.publication.pubmed.dao

import org.batteryparkdev.neo4j.service.Neo4jConnectionService
import org.batteryparkdev.neo4j.service.Neo4jUtils
import org.batteryparkdev.placeholder.model.NodeIdentifier
import org.batteryparkdev.publication.pubmed.model.PubMedEntry
import org.batteryparkdev.publication.pubmed.service.PubMedRetrievalService
import org.neo4j.driver.Record
import arrow.core.Either
import org.batteryparkdev.logging.service.LogService
import org.batteryparkdev.publication.pubmed.loader.PubMedNodeLoader

object PubMedPublicationDao {
    private const val mergePubMedArticleTemplate = "MERGE (pub:Publication { pub_id: PUBID}) " +
            "SET  pub.pmc_id = \"PMCID\", pub.doi_id = \"DOIID\", " +
            " pub.journal_name = \"JOURNAL_NAME\", pub.journal_issue = \"JOURNAL_ISSUE\", " +
            " pub.title = \"TITLE\",  " +
            " pub.author = \"AUTHOR\", pub.reference_count = REFCOUNT, " +
            " pub.cited_by_count = CITED_BY " +
            "  RETURN pub.pub_id"

    private const val emptyPublicationNodeQuery =
        "MATCH (pub) WHERE (pub:PubMed OR pub:Reference) AND pub.title =\"\" " +
                " return pub.pub_id"

    /*
    Template to create a new Section node containing the abstract
     */
    private const val mergeSectionNodeTemplate = "MERGE (s:PublicationSection{ section_id: SECID}) " +
            "SET s.text = ABSTRACT RETURN s.section_id"

    /*
    Private function to generate the next section id for a specified Publication
    (eg. 26050619-1)
     */
    private fun getPublicationSectionId(pubId: String): String {
        val cypher = "MATCH (p:Publication{p: ${Neo4jUtils.formatPropertyValue(pubId)} }) -- " +
                "(PublicationSection) RETURN COUNT(PublicationSection.section_id) +1"
        return pubId.plus("-").plus(Neo4jConnectionService.executeCypherCommand(cypher))
    }

    /*
    Template to create a relationship between the Publication node and a Section node with a property
    type=abstract
     */
    private const val sectionRelationshipTemplate = "MATCH (p:Publication), (s:PublicationSection) " +
            " WHERE p.pub_id = PUBID AND s.section_id = SECTIONID " +
            " CREATE (p) -[r:HAS_SECTION {type: TYPE} ]-> (s)"


    fun loadPubmedEntry(pubMedEntry: PubMedEntry): String {
        val pubId = mergePubmedEntryNode(pubMedEntry)
        val sectionId = when (pubMedEntry.abstract.isNotEmpty()) {
            true -> mergePublicationSection(pubMedEntry)
            false -> ""
        }
        return pubId.plus(":").plus(sectionId)
    }
/*
Private function to create a Publication/PubMed node
 */
    private fun mergePubmedEntryNode(pubMedEntry: PubMedEntry): String {
        val merge = mergePubMedArticleTemplate.replace("PUBID", pubMedEntry.pubmedId.toString())
            .replace("PMCID", pubMedEntry.pmcId)
            .replace("DOIID", pubMedEntry.doiId)
            .replace("JOURNAL_NAME", pubMedEntry.journalName)
            .replace("JOURNAL_ISSUE", pubMedEntry.journalIssue)
            .replace("TITLE", pubMedEntry.articleTitle)
            //.replace("ABSTRACT", pubMedEntry.abstract)
            .replace("AUTHOR", pubMedEntry.authorCaption)
            .replace("REFCOUNT", pubMedEntry.referenceSet.size.toString())
            .replace("CITED_BY", pubMedEntry.citedByCount.toString())
        return Neo4jConnectionService.executeCypherCommand(merge)
    }
    /*
    Private function to create a PublicationSection node and a relationship to the
    Publication node
     */
    private fun mergePublicationSection(pubMedEntry: PubMedEntry): String {
        val abstract = pubMedEntry.abstract
        val pubId = pubMedEntry.pubmedId.toString()
        val merge = mergeSectionNodeTemplate.replace(
            "SECID",
            Neo4jUtils.formatPropertyValue(getPublicationSectionId(pubId))
        )
            .replace("ABSTRACT", Neo4jUtils.formatPropertyValue(abstract))
        val secId = Neo4jConnectionService.executeCypherCommand(merge)
        val relate = sectionRelationshipTemplate.replace("PUBID", Neo4jUtils.formatPropertyValue(pubId))
            .replace("SECTIONID", secId)
            .replace("TYPE", "\"Abstract\"")
        Neo4jConnectionService.executeCypherCommand(relate)
        return secId

    }

    fun publicationNodeExistsPredicate(pubId: String): Boolean =
        Neo4jUtils.nodeExistsPredicate(
            NodeIdentifier(
                "Publication", "pub_id",
                Neo4jUtils.formatPropertyValue(pubId)
            )
        )

    fun resolvePlaceholderPubMedNodes(): Sequence<String> =
        Neo4jConnectionService.executeCypherQuery(emptyPublicationNodeQuery)
            .map { rec -> resolvePubMedIdentifier(rec) }
            .toList().asSequence()

    /*
   Private function to map selected items from an
   empty PubMed node into a String
    */
    private fun resolvePubMedIdentifier(record: Record): String =
        record.asMap()["pub.pub_id"].toString()

}

fun main() {
    val pubId = "26050619"
    when (val retEither = PubMedRetrievalService.retrievePubMedArticle("26050619")) {
        is Either.Right -> {
            val publication = retEither.value
            println("Title: ${publication.medlineCitation.article.articleTitle.getvalue()}")
           val entry = PubMedEntry.parsePubMedArticle(publication)
            PubMedPublicationDao.loadPubmedEntry(entry)
        }
        is Either.Left -> {
            LogService.logException( retEither.value)
        }
    }
}