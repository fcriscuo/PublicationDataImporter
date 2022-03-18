package org.batteryparkdev.service

import arrow.core.Either
import org.batteryparkdev.logging.service.LogService
import org.batteryparkdev.publication.pubmed.service.PubMedRetrievalService

class TestPubMedRetrievalService {
}
fun main() {
    // test PubMedArticle retrieval
    when (val retEither = PubMedRetrievalService.retrievePubMedArticle("26050619")) {
        is Either.Right -> {
            val publication = retEither.value
            println("Title: ${publication.medlineCitation.article.articleTitle.getvalue()}")
            val pubmedId = publication.medlineCitation.pmid.getvalue()
            PubMedRetrievalService.retrieveCitationIds(pubmedId).stream()
                .forEach { cit -> println(cit) }
            PubMedRetrievalService.generateReferencePlaceholderNodes(pubmedId.toInt()).stream()
                .forEach { ref -> println("Placeholder $ref") }
        }
        is Either.Left -> {
            LogService.logException( retEither.value)
        }
    }
    // NCBI stress test
    // Registered account allows 10 requests/second
    var counter = 0
    repeat(10){
        counter += 1
        PubMedRetrievalService.retrievePubMedArticle("26050619")
        PubMedRetrievalService.retrieveReferenceIds("26050619")
        PubMedRetrievalService.retrieveCitationIds("26050619")
        println("loop $counter")
        Thread.sleep(PubMedRetrievalService.ncbiDelay)
    }
}