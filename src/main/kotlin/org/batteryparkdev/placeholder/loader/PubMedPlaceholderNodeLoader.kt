package org.batteryparkdev.placeholder.loader

import org.batteryparkdev.logging.service.LogService
import org.batteryparkdev.neo4j.service.Neo4jConnectionService
import org.batteryparkdev.neo4j.service.Neo4jUtils
import org.batteryparkdev.nodeidentifier.dao.NodeIdentifierDao
import org.batteryparkdev.nodeidentifier.model.NodeIdentifier
import org.batteryparkdev.nodeidentifier.model.RelationshipDefinition
import org.batteryparkdev.publication.pubmed.service.PubMedRetrievalService

/*
Responsible for persisting a Publication Id as a placeholder node in the
Neo4j database. If the id represents an origin publication (i.e. not
a reference), placeholder nodes for the publications references will
also be created. This class also accommodates existing database nodes
that serve another role (e.g. a PubMed Publication that is also used
as a Reference by another PubMed Publication)
 */
class PubMedPlaceholderNodeLoader(
    private val pubId: String, private val parentId: String,
    private val parentLabel: String, private val parentIdProperty: String
) {
    private val pubmedLabel = "PubMed"
    private val publicationLabel = "Publication"
    private val emptyPropertyName = "title"
    private val referenceLabel = "Reference"
    private val pubIdProperty = "pub_id"
    private val publicationRelationship = "HAS_PUBLICATION"
    private val referenceRelationship = "HAS_REFERENCE"

    fun registerPubMedPublication() {
        // check if this node id has been previously loaded
        when (Neo4jUtils.nodeExistsPredicate(NodeIdentifier(publicationLabel,pubIdProperty, pubId ))) {
            true -> updateExistingPublicationNode()
            false -> createNewPublicationNode()
        }
    }

    private fun updateExistingPublicationNode() {
        // determine if the existing node already has a PubMed label
        // that means its References have already been created
        // otherwise it means that the current node was formerly labeled as a Reference node
        // but is now also being labelled a PubMed node and its References need to be
        // retrieved
        val isExistingPubMedNode = Neo4jUtils.nodeExistsPredicate(NodeIdentifier(publicationLabel,pubIdProperty, pubId ))
        createNewPublicationNode()
        // load references
        if (isExistingPubMedNode.not()) {
            loadPubMedReferences()
        }
    }

    /*
    Private method to create a new PublicationNode (i.e. placeholder)
     */
    private fun createNewPublicationNode() {
        val parentIdentifier = NodeIdentifier(parentLabel, parentIdProperty, parentId)
        val childIdentifier = NodeIdentifier(publicationLabel, pubIdProperty, pubId, pubmedLabel)
        val publicationNode = RelationshipDefinition(
            parentIdentifier, childIdentifier,
            publicationRelationship
        )
        NodeIdentifierDao.defineRelationship(publicationNode)
        loadPubMedReferences()
    }

    /*
    Private method to add placeholder publication nodes for all the articles
    referenced in the current PubMed article
     */
    private fun loadPubMedReferences() {
        println("##### loadPubMedReferences invoked for $pubId")
        PubMedRetrievalService.generateReferencePlaceholderNodes(pubId.toInt())
            .forEach { refDef ->
                run {
                    println("Reference id:  ${refDef.childNode.idValue}  label = ${refDef.relationshipType}")
                    NodeIdentifierDao.defineRelationship(refDef)
                }
            }
    }
}