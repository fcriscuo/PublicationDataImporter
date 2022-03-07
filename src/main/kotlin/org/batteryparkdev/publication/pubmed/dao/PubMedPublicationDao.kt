package org.batteryparkdev.publication.pubmed.dao

import org.batteryparkdev.neo4j.service.Neo4jConnectionService
import org.batteryparkdev.publication.pubmed.model.PubMedEntry

object PubMedPublicationDao {
    private const val mergePubMedArticleTemplate = "MERGE (pub:Publication { pub_id: PMAID}) " +
            "SET  pub.pmc_id = \"PMCID\", pub.doi_id = \"DOIID\", " +
            " pub.journal_name = \"JOURNAL_NAME\", pub.journal_issue = \"JOURNAL_ISSUE\", " +
            " pub.article_title = \"TITLE\", pub.abstract = \"ABSTRACT\", " +
            " pub.author = \"AUTHOR\", pub.reference_count = REFCOUNT, " +
            " pub.cited_by_count = CITED_BY " +
            "  RETURN pub.pub_id"

    fun mergePubMedEntry(pubMedEntry: PubMedEntry): String {
        val merge = mergePubMedArticleTemplate.replace("PMAID", pubMedEntry.pubmedId.toString())
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

}