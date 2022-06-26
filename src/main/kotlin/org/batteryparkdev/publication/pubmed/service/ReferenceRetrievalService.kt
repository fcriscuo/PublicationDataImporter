package org.batteryparkdev.publication.pubmed.service

import arrow.core.Either
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.batteryparkdev.neo4j.service.Neo4jConnectionService
import org.batteryparkdev.neo4j.service.Neo4jUtils
import org.batteryparkdev.nodeidentifier.model.NodeIdentifier
import org.batteryparkdev.publication.pubmed.dao.PubMedPublicationDao
import org.batteryparkdev.publication.pubmed.model.PubMedEntry
import org.batteryparkdev.publication.pubmed.service.PubMedRetrievalService

/*
Represents a uservice that will retrieve the PubMed references for a specified 
PubMed Id. If the PubMed Id for a reference is novel
a new Neo4j Publication node will be loaded. In all cases a
Publication/PubMed -[HAS_REFERENCE] -> Publication/Reference relationship will be established
If an existing Publication/PubMed node is also a Reference, a Reference label will be added
to that node (i.e. Publication/PubMed/Reference)
 */
class ReferenceRetrievalService(val pubId: Int) {

    fun processReferences() = runBlocking {
        var pubCount = 0
        val entries =
            createReferenceRelationship(
                addReferenceLabel(
                    loadReferenceNodes(
                        generatePubMedEntries(
                            resolveReferenceIds()
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
    /*
    Retrieve the PubMed Ids for Publications referenced by the supplied Publication
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.resolveReferenceIds() =
        produce<Pair<Int, List<Int>>> {
            val idSet = PubMedRetrievalService.retrieveReferenceIds(pubId.toString())
            if (idSet.isNotEmpty()) {
                send(Pair(pubId, idSet.toList()))
            }
            delay(300L)
        }
    
    /*
    Retrieve referenced PubMed Articles using a batch request
    */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.generatePubMedEntries(pairs: ReceiveChannel<Pair<Int, List<Int>>>) =
        produce<PubMedEntry> {
            for (pair in pairs) {
                val pubId = pair.first
                val idList = pair.second
                val ids = idList.filter { it -> PubMedPublicationDao.referenceNodeExistsPredicate(it.toString()).not() }
                    .map(Int::toString)
                // retrieve these ids
                when (val retEither = PubMedRetrievalService.retrievePubMedArticleBatch(ids.toSet())) {
                    is Either.Right -> {
                        retEither.value
                            .map { article -> PubMedEntry.parsePubMedArticle(article, "Reference", pubId) }
                            .forEach {
                                send(it)
                            }
                    }

                    is Either.Left -> {
                        println("Exception ${retEither.value.toString()}")
                    }
                }
                delay(20L)
            }
        }

//    /*
//    3. For each reference PubMed Id, if novel, generate a PubMedEntry object and create a Publication/Reference node
//     val pubMedEntry = PubMedEntry.parsePubMedArticle(retEither.value, pubmedNode.primaryLabel)
//                val newPubMedId = PubMedPublicationDao.loadPubmedEntry(pubMedEntry)
//     */

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

}
