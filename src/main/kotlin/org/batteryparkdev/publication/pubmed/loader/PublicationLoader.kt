package org.batteryparkdev.publication.pubmed.loader

import arrow.core.Either
import com.google.common.base.Stopwatch
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.batteryparkdev.genomicgraphcore.common.service.LogService
import org.batteryparkdev.publication.pubmed.dao.PubMedPublicationDao
import org.batteryparkdev.publication.pubmed.model.PubMedEntry
import org.batteryparkdev.publication.pubmed.service.PubMedRetrievalService
import org.batteryparkdev.publication.pubmed.service.ReferenceRetrievalService
import java.util.concurrent.TimeUnit

/*
Kotlin application responsible for scanning a Neo4j database for Publication placeholder nodes.
a placeholder node is defined as a node with a label(s) and identifier property as well as relationships
to other nodes already defined. In the case of Publication/PubMed node that means a Publication node
with a PubMed label, a PubMed Id, and a relationship to a parent node defined. This application will query
the Neo4j database for these nodes and retrieve their entries from NCBI to complete the nodes properties.
It will also create complete Publication/Reference nodes for a Publication/PubMed node's references.
Because NCBI enforces a 3 requests/second (10 requests/second for registered users), this process take a
considerable amount of time. Please note that the application can be restarted if the NCBI resource become
unavailable.

 */

class PublicationLoader() {
    /*
    Private function to query the Neo4j database for empty nodes
    and producing a channel of PubMed Ids (String)
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.getPublicationPlaceholders() =
        produce<String> {
            PubMedPublicationDao.resolvePlaceholderPubMedNodes()
                .forEach { send(it) }
            delay(10)
        }

    /*
    Private function to retrieve PubMed data from NCBI and
    map it to a PubMedEntry model object
    Input is a channel of String objects representing PubMed Ids
    Output is a channel of PubMedEntry objects
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.retrievePubMedData(identifiers: ReceiveChannel<String>) =
        produce<PubMedEntry> {
            for (identifier in identifiers) {

                val entry = generatePubMedEntries(identifier)
                if (entry != null) {
                    send(entry)
                    delay(20)
                }
            }
        }

    /*
   Private function to request PubMed data from NCBI based on
   a specified PubMed Id
    */
    private fun generatePubMedEntries(identifier: String): PubMedEntry? {
        return when (val retEither = PubMedRetrievalService.retrievePubMedArticle(identifier)) {
            is Either.Right -> {
                val pubmedArticle = retEither.value
                PubMedEntry.parsePubMedArticle(pubmedArticle)
            }
            is Either.Left -> {
                LogService.exception(retEither.value)
                null
            }
        }
    }

    /*
   Function to load retrieved data into the Neo4j database
    */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.loadPubMedEntries(entries: ReceiveChannel<PubMedEntry>) =
        produce<PubMedEntry> {
            for (entry in entries) {
                PubMedPublicationDao.loadPubmedEntry(entry)
                println("Publication node for PubMed Id ${entry.pubmedId} loaded")
                send(entry)
                delay(20)
            }
        }

    /*
    Load Publication/Reference nodes for reference articles
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.loadPublicationReferenceNodes(entries: ReceiveChannel<PubMedEntry>) =
        produce<PubMedEntry> {
            for (entry in entries) {
                ReferenceRetrievalService(entry.pubmedId).processReferences()
                send(entry)
                delay(20L)
            }
        }

    fun processPlaceholderNodes() = runBlocking {
        var nodeCount = 0
        var cycleCount = 0
        LogService.info("Completing placeholder PubMedArticle nodes")
        val stopwatch = Stopwatch.createStarted()
        repeat(2) {
            cycleCount += 1
            val ids =
                loadPublicationReferenceNodes(
                    loadPubMedEntries(
                        retrievePubMedData(
                            getPublicationPlaceholders()
                        )
                    )
                )
            for (id in ids) {
                nodeCount += 1
            }
            delay(100)
        }
        LogService.info(
            "Publication data loaded " +
                    " $nodeCount nodes in " +
                    " ${stopwatch.elapsed(TimeUnit.SECONDS)} seconds"
        )
    }
}

fun main(){
    PublicationLoader().processPlaceholderNodes()
}

