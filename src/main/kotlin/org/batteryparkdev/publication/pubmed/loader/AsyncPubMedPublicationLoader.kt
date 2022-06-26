package org.batteryparkdev.publication.pubmed.loader

import arrow.core.Either
import com.google.common.base.Stopwatch
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.batteryparkdev.logging.service.LogService
import org.batteryparkdev.publication.pubmed.dao.PubMedPublicationDao
import org.batteryparkdev.publication.pubmed.model.PubMedEntry
import org.batteryparkdev.publication.pubmed.service.PubMedRetrievalService
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.concurrent.scheduleAtFixedRate

/*
Object responsible for scanning the Neo4j for placeholder (i.e. empty) PubMed & Reference
nodes and retrieving their complete entries from NCBI. The rationale for this design is to
avoid impeding the loading of other data types into Neo4j by the time-consuming process of
loading PubMed data. This is because NCBI strictly enforces a maximum retrial request rate
of 3 requests/second (10 requests/second if a registered NCBI user).
 */
object AsyncPubMedPublicationLoader {
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
                // logger.atInfo().log("Retrieving data for ${identifier.pubmedId}")
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
                LogService.logException(retEither.value)
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
                println("Loaded complete Publication node for PubMed Id ${entry.pubmedId}")
                send(entry)
                delay(20)
            }
        }

    /*
    If this Publication is not a Reference, then generate placeholder nodes for its References
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.loadReferencePlaceholderNodes(entries: ReceiveChannel<PubMedEntry>) =
        produce<PubMedEntry> {
            for (entry in entries) {
                if (PubMedPublicationDao.referenceNodeExistsPredicate(entry.pubmedId.toString()).not()) {
                    PubMedRetrievalService.generateReferencePlaceholderNodes(entry.pubmedId)
                    println("Generating reference placeholder nodes for PubMed Id ${entry.pubmedId}")
                }
                send(entry)
                delay(20L)
            }
        }

    /*
    Public function to periodically query the Neo4j database for placeholder
    PubMed nodes and retrieve the remaining properties from PubMed at NCBI
     */
    fun scheduledPlaceHolderNodeScan(interval: Long = 60_000): TimerTask {
        val fixedRateTimer = Timer().scheduleAtFixedRate(delay = 5_000, period = interval) {
            processPlaceholderNodes()
        }
        return fixedRateTimer
    }

    private fun processPlaceholderNodes() = runBlocking {
        var nodeCount = 0
        var cycleCount = 0
        LogService.logInfo("Completing placeholder PubMedArticle nodes")
        val stopwatch = Stopwatch.createStarted()
        repeat(2) {
            cycleCount += 1
            val ids =
                loadReferencePlaceholderNodes(
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
        LogService.logInfo(
            "Publication data loaded " +
                    " $nodeCount nodes in " +
                    " ${stopwatch.elapsed(TimeUnit.SECONDS)} seconds"
        )
    }
}

