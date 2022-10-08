package org.batteryparkdev.service

import ai.wisecube.pubmed.PubmedArticle
import arrow.core.Either
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.batteryparkdev.publication.pubmed.dao.PubMedPublicationDao
import org.batteryparkdev.publication.pubmed.model.PubMedEntry
import org.batteryparkdev.publication.pubmed.service.PubMedRetrievalService

class TestReferenceRetrievalUtility {

    /*
   1. Retrieve the PubMed Ids for all Publication/PubMed nodes
    */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.queryPubMedIds() =
        produce<Int> {
            PubMedPublicationDao.gatAllPubmedIds()
                .forEach {
                    send(it.toInt())
                    delay(10)
                }
        }

    /*
    2. For each retrieve PubMed node, retrieve the PubMed Ids for its references
    fun retrieveReferenceIds(pubmedId: String): Set<Int>
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.resolveReferenceIds(ids: ReceiveChannel<Int>) =
        produce<List<Int>> {
            for (id in ids) {
                val idSet = PubMedRetrievalService.retrieveReferenceIds(id.toString())
                if (idSet.size > 0) {
                    send(idSet.toList())
                }
                delay(300L)
            }
        }

    /*
    Retrive PubMed Articles by
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.fetchPubMedArticles(idLists: ReceiveChannel<List<Int>>) =
        produce<PubmedArticle> {
            for (idList in idLists) {
                val ids = idList.map(Int::toString)
                // retrieve these ids
                when (val retEither = PubMedRetrievalService.retrievePubMedArticleBatch(ids.toSet())) {
                    is Either.Right -> {
                        retEither.value.forEach { it -> send(it) }
                    }

                    is Either.Left -> {
                        println("Exception ${retEither.value.toString()}")
                    }
                }
            }
        }
    /*
    Generate PubMedEntry objects
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.generatePubMedEntry(articles: ReceiveChannel<PubmedArticle>) =
        produce<PubMedEntry> {
            for(article in articles) {
                send(PubMedEntry.parsePubMedArticle(article, "Reference"))
                delay(20)
            }
        }

    fun testBatch() = runBlocking {
        val entries = generatePubMedEntry(
            fetchPubMedArticles(
            resolveReferenceIds(
                queryPubMedIds()
            )
        )
        )

        for (entry in entries){
            println("PubMedEntry:  ${entry.pubmedId} label: ${entry.label}  title:  ${entry.articleTitle}")
        }
    }
}
fun main() {
    TestReferenceRetrievalUtility().testBatch()
}