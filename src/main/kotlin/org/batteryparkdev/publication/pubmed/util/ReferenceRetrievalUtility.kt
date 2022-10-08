package org.batteryparkdev.publication.pubmed.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.batteryparkdev.publication.pubmed.dao.PubMedPublicationDao
import org.batteryparkdev.publication.pubmed.service.ReferenceRetrievalService

/*
Represents a utility application that will retrieve the PubMed references for
PubMed nodes in the Neo4j database. If the reference PubMed Id for a reference is novel
a new Neo4j Publication node will be loaded. In all cases a
Publication/PubMed -[HAS_REFERENCE] -> Publication/Reference relationship will be established
If an existing Publication/PubMed node is also a Reference, a Reference label will be added
to that node (i.e. Publication/PubMed/Reference)
 */
class ReferenceRetrievalUtility {

    fun loadPubMedReferences() = runBlocking {
        var pubCount = 0
        val ids =
            resolveReferenceIds(
                queryPubMedIds()
            )
        for (id in ids) {
            pubCount += 1
            println("Processed references for PubMed Id: $id")
        }
        println("Completed reference retrieval for $pubCount articles")
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.queryPubMedIds() =
        produce<Int> {
            PubMedPublicationDao.gatAllPubmedIds().forEach {
                send(it.toInt())
                delay(10)
            }
        }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.resolveReferenceIds(ids: ReceiveChannel<Int>) =
        produce<Int> {
            for (id in ids) {
               ReferenceRetrievalService(id).processReferences()
                send(id)
                delay(300L)
            }
        }
}

fun main() = ReferenceRetrievalUtility().loadPubMedReferences()