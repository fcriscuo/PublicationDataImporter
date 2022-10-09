package org.batteryparkdev.publication.pubmed.loader

import arrow.core.Either
import org.batteryparkdev.genomicgraphcore.common.service.LogService
import org.batteryparkdev.genomicgraphcore.common.service.log
import org.batteryparkdev.genomicgraphcore.neo4j.nodeidentifier.NodeIdentifier
import org.batteryparkdev.genomicgraphcore.neo4j.nodeidentifier.NodeIdentifierDao
import org.batteryparkdev.genomicgraphcore.neo4j.nodeidentifier.RelationshipDefinition
import org.batteryparkdev.genomicgraphcore.neo4j.service.Neo4jUtils

import org.batteryparkdev.publication.pubmed.dao.PubMedPublicationDao
import org.batteryparkdev.publication.pubmed.model.PubMedEntry
import org.batteryparkdev.publication.pubmed.service.PubMedRetrievalService
import org.batteryparkdev.publication.pubmed.service.ReferenceRetrievalService

/*
Responsible for loading data for a specific PubMed Id into the Neo4j database
as a single Publication/PubMed node and >= 0 Publication/Reference nodes
The data are only loaded if they represent novel nodes
This allows an application to load a relatively small number of PubMed nodes
without invoking an asynchronous data loading thread
 */
class PubMedNodeLoader() {
    private val pubmedLabel = "PubMed"
    private val publicationLabel = "Publication"
    private val referenceLabel = "Reference"
    private val pubIdProperty = "pub_id"
    private val publicationRelationship = "HAS_PUBLICATION"
    private val referenceRelationship = "HAS_REFERENCE"

    /*
    Public function to load a novel PubMed publication into the Neo4j database
    It will also create a parent to child relationship if novel, regardless of
    whether the supplied PubMed Id is novel
     */
    fun loadPubMedNode(parentNode: NodeIdentifier, pubmedNode: NodeIdentifier) {

        LogService.info("Loading Publication ${pubmedNode.primaryLabel} second Label: ${pubmedNode.secondaryLabel} PubId: ${pubmedNode.idValue}  " +
                " \n Parent: ${parentNode.primaryLabel}  second label: ${parentNode.secondaryLabel}  id= ${parentNode.idValue}")

        when (PubMedPublicationDao.pubmedNodeExistsPredicate(pubmedNode.idValue)) {
            false -> createPublicationNode(pubmedNode)
            true -> when (novelPubMedLabel(pubmedNode)){
                true -> loadPubMedReferences(pubmedNode)
                false -> Neo4jUtils.addLabelToNode(pubmedNode)
            }
        }
        Neo4jUtils.addLabelToNode(pubmedNode)
        createParentRelationship(parentNode, pubmedNode)
    }

    /*
    Function to create a HAS_PUBLICATION or HAS_REFERENCE relationship depending
    on the parent node type
     */
    private fun createParentRelationship(parentNode: NodeIdentifier, pubmedNode: NodeIdentifier) {
        val relationship = when (parentNode.secondaryLabel.equals(pubmedLabel)){
            true -> referenceRelationship
            false -> publicationRelationship
        }
        LogService.info("Creating ${parentNode.primaryLabel} - $relationship -> ${pubmedNode.secondaryLabel}")
       NodeIdentifierDao.defineRelationship(RelationshipDefinition( parentNode,pubmedNode,relationship))
    }

   /*
   Function to determine if an existing Reference node is now
   has a new PubMed label
   If so, its references need to be loaded
    */
   private fun novelPubMedLabel(pubmedNode: NodeIdentifier):Boolean =
        PubMedPublicationDao.pubmedNodeExistsPredicate(pubmedNode.idValue).not()
            .and(pubmedNode.secondaryLabel == pubmedLabel)

    private fun createPublicationNode(pubmedNode: NodeIdentifier){
        LogService.info("Creating new node for ${pubmedNode.primaryLabel}l:${pubmedNode.idValue}")
        when (val retEither = PubMedRetrievalService.retrievePubMedArticle(pubmedNode.idValue)) {
            is Either.Right -> {
                val pubMedEntry = PubMedEntry.parsePubMedArticle(retEither.value, pubmedNode.primaryLabel)
                val newPubMedId = PubMedPublicationDao.loadPubmedEntry(pubMedEntry)
                LogService.info("PubMed Id $newPubMedId  loaded into Neo4j")
                loadPubMedReferences(pubmedNode)
            }
            is Either.Left -> {
                retEither.value.log()
            }
        }
    }
    /*
    Private function to load Publication/Reference nodes for a
    Publication/PubMed node
     */
    private fun loadPubMedReferences(pubmedNode: NodeIdentifier) =
       ReferenceRetrievalService(pubmedNode.idValue.toInt()).processReferences()
}
