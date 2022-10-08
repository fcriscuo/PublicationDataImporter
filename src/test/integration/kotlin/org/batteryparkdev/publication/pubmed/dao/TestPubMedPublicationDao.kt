package org.batteryparkdev.publication.pubmed.dao

import arrow.core.Either
import org.batteryparkdev.logging.service.LogService
import org.batteryparkdev.publication.pubmed.model.PubMedEntry
import org.batteryparkdev.publication.pubmed.service.PubMedRetrievalService

class TestPubMedPublicationDao {
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