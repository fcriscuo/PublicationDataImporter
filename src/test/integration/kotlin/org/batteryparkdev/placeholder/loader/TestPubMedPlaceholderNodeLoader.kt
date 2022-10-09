package org.batteryparkdev.placeholder.loader

import org.batteryparkdev.genomicgraphcore.neo4j.nodeidentifier.NodeIdentifier
import org.batteryparkdev.genomicgraphcore.neo4j.service.Neo4jUtils


class TestPubMedPlaceholderNodeLoader {
    val pubIdList = listOf("21876726", "12021310","9585234")
    val geneMutationCollectionLabel = "GeneMutationCollection"
    val gmcIdProperty = "gene_symbol"
    val gmcId = "ALK"

    private fun deleteNodes() {
       pubIdList.forEach { id -> Neo4jUtils.deleteNodeById(NodeIdentifier("Publication","pub_id", id)) }
    }
    fun createPlaceholderNodes () {
        pubIdList.forEach { pub -> run{
            Neo4jUtils.deleteNodeById(NodeIdentifier("Publication","pub_id", pub)) }
            PubMedPlaceholderNodeLoader(pub, gmcId, geneMutationCollectionLabel, gmcIdProperty).registerPubMedPublication()
        } }
    }

    fun main() {
      TestPubMedPlaceholderNodeLoader().createPlaceholderNodes()
}

