package org.batteryparkdev.publication.pubmed.util

import arrow.core.Either
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.batteryparkdev.logging.service.LogService
import org.batteryparkdev.neo4j.service.Neo4jConnectionService
import org.batteryparkdev.neo4j.service.Neo4jUtils
import org.batteryparkdev.nodeidentifier.model.NodeIdentifier
import org.batteryparkdev.publication.pubmed.dao.PubMedPublicationDao
import org.batteryparkdev.publication.pubmed.model.PubMedEntry
import org.batteryparkdev.publication.pubmed.service.PubMedRetrievalService

/*
Represents a utility application that will retrieve the PubMed references for
PubMed nodes in the Neo4j database. If the reference PubMed Id for a reference is novel
a new Neo4j Publication node will be loaded. In all cases a
Publication/PubMed -[HAS_REFERENCE] -> Publication/Reference relationship will be established
If an existing Publication/PubMed node is also a Reference, a Reference label will be added
to that node (i.e. Publication/PubMed/Reference)
 */
class ReferenceRetrievalUtility {


    /*
    1. Retrieve the PubMed Ids for all Publication/PubMed nodes
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.queryPubMedIds() =
        produce<Int> {
            PubMedPublicationDao.gatAllPubmedIds().forEach {
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
        produce<Pair<Int, Set<Int>>> {
            for (id in ids) {
                send(Pair(id, PubMedRetrievalService.retrieveReferenceIds(id.toString())))
                delay(300L)
            }
        }

    /*
    3. For each reference PubMed Id, if novel, retrieve the PubMed entry and create a Publication/Reference node
     val pubMedEntry = PubMedEntry.parsePubMedArticle(retEither.value, pubmedNode.primaryLabel)
                val newPubMedId = PubMedPublicationDao.loadPubmedEntry(pubMedEntry)
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.retrieveNovelReferences(refPairs: ReceiveChannel<Pair<Int, Set<Int>>>) =
        produce<PubMedEntry> {
            for (refPair in refPairs) {
                val pubId = refPair.first
                val refSet = refPair.second
                refSet.filter { ref -> PubMedPublicationDao.referenceNodeExistsPredicate(ref.toString()).not() }
                    .map { ref -> resolveReferencePubMedEntry(ref, pubId) }
                    .filterNotNull()
                    .forEach {
                        send(it)
                        delay(20L)
                    }
            }
        }

    /*
    load the new Reference node into Neo4j
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.loadReferenceNodes(pubmedEntries: ReceiveChannel<PubMedEntry>) =
        produce<PubMedEntry> {
            for (entry in pubmedEntries) {
                PubMedPublicationDao.loadPubmedEntry(entry)
                send(entry)
                delay(30L)
            }
        }

    private fun resolveReferencePubMedEntry(refId: Int, pubId: Int): PubMedEntry? {
        when (val retEither = PubMedRetrievalService.retrievePubMedArticle(refId.toString())) {
            is Either.Right -> {
                return PubMedEntry.parsePubMedArticle(retEither.value, "Reference", pubId)
            }

            is Either.Left -> {
                LogService.logException(retEither.value)
            }
        }
        return null
    }

    /*
    4. Ensure that the existing Publication node has a Reference label
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.addReferenceLabel(pubmedEntries: ReceiveChannel<PubMedEntry>) =
        produce<PubMedEntry> {
            for (entry in pubmedEntries) {
                val refIdentifier = NodeIdentifier(
                    "Publication", "pub_id",
                    entry.pubmedId.toString(), "Reference"
                )
                Neo4jUtils.addLabelToNode(refIdentifier)
                send(entry)
                delay(20L)
            }
        }

    /*
    5. Create a Publication/PubMed -[HAS_REFERENCE] -> Publication/Reference relationship
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.createReferenceRelationship(pubmedEntries: ReceiveChannel<PubMedEntry>) =
        produce<PubMedEntry> {
            for (entry in pubmedEntries) {
                Neo4jConnectionService.executeCypherCommand(generateRelationshipCypher(entry))
                send(entry)
                delay(20L)
            }
        }

    private fun generateRelationshipCypher(entry: PubMedEntry): String =
        "MATCH (pm:PubMed{pub_id: ${entry.parentPubMedId}}) " +
                "MATCH (ref:Reference{pub_id: ${entry.pubmedId}}) " +
                "MERGE (pm) -[rel:HAS_REFERENCE] -> (ref) RETURN rel "

    fun processReferences() = runBlocking {
        var pubCount = 0
        val entries =
            createReferenceRelationship(
                addReferenceLabel(
                    loadReferenceNodes(
                        retrieveNovelReferences(
                            resolveReferenceIds(
                                queryPubMedIds()
                            )
                        )
                    )
                )
            )
        for (entry in entries) {
            pubCount += 1
            println("Reference Id: ${entry.pubmedId}   label: ${entry.label}  PubMed Id: ${entry.parentPubMedId}")
        }
        println("Publication count = $pubCount")

    }
}

fun main() = ReferenceRetrievalUtility().let {
    it.processReferences()
}