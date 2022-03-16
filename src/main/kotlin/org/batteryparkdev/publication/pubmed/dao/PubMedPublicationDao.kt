package org.batteryparkdev.publication.pubmed.dao

import org.batteryparkdev.neo4j.service.Neo4jConnectionService
import org.batteryparkdev.neo4j.service.Neo4jUtils
import org.batteryparkdev.placeholder.model.NodeIdentifier
import org.batteryparkdev.publication.pubmed.model.PubMedEntry
import org.neo4j.driver.Record

object PubMedPublicationDao {
    private const val mergePubMedArticleTemplate = "MERGE (pub:Publication { pub_id: PUBID}) " +
            "SET  pub.pmc_id = \"PMCID\", pub.doi_id = \"DOIID\", " +
            " pub.journal_name = \"JOURNAL_NAME\", pub.journal_issue = \"JOURNAL_ISSUE\", " +
            " pub.title = \"TITLE\", pub.abstract = \"ABSTRACT\", " +
            " pub.author = \"AUTHOR\", pub.reference_count = REFCOUNT, " +
            " pub.cited_by_count = CITED_BY " +
            "  RETURN pub.pub_id"

    private const val emptyPublicationNodeQuery = "MATCH (pub) WHERE (pub:PubMed OR pub:Reference) AND pub.title =\"\" " +
            " return pub.pub_id"

    fun mergePubMedEntry(pubMedEntry: PubMedEntry): String {
        val merge = mergePubMedArticleTemplate.replace("PUBID", pubMedEntry.pubmedId.toString())
            .replace("PMCID", pubMedEntry.pmcId)
            .replace("DOIID", pubMedEntry.doiId)
            .replace("JOURNAL_NAME", pubMedEntry.journalName)
            .replace("JOURNAL_ISSUE", pubMedEntry.journalIssue)
            .replace("TITLE",  pubMedEntry.articleTitle)
            .replace("ABSTRACT", pubMedEntry.abstract)
            .replace("AUTHOR", pubMedEntry.authorCaption)
            .replace("REFCOUNT", pubMedEntry.referenceSet.size.toString())
            .replace("CITED_BY", pubMedEntry.citedByCount.toString())
        return Neo4jConnectionService.executeCypherCommand(merge)
    }

    fun publicationNodeExistsPredicate(pubId:String): Boolean =
        Neo4jUtils.nodeExistsPredicate(NodeIdentifier("Publication","pub_id",
            Neo4jUtils.formatPropertyValue(pubId) ))

    fun resolvePlaceholderPubMedNodes(): Sequence<String> =
        Neo4jConnectionService.executeCypherQuery(emptyPublicationNodeQuery)
            .map{rec -> resolvePubMedIdentifier(rec)}
            .toList().asSequence()

    /*
   Private function to map selected items from an
   empty PubMed node into a String
    */
    private fun resolvePubMedIdentifier(record: Record): String =
        record.asMap()["pub.pub_id"].toString()

}