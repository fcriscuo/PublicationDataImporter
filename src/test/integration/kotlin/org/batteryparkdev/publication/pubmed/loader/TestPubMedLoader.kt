package org.batteryparkdev.publication.pubmed.loader

import org.batteryparkdev.genomicgraphcore.common.formatNeo4jPropertyValue
import org.batteryparkdev.genomicgraphcore.common.service.LogService
import org.batteryparkdev.genomicgraphcore.neo4j.nodeidentifier.NodeIdentifier
import org.batteryparkdev.genomicgraphcore.neo4j.service.Neo4jConnectionService
import org.batteryparkdev.genomicgraphcore.neo4j.service.Neo4jUtils
import org.batteryparkdev.publication.pubmed.dao.PubMedPublicationDao

class TestPubMedLoader {
   private  val parentNode = NodeIdentifier("Parent","parent_id", "Parent123" )
    private val pubmedNode = NodeIdentifier("Publication", "pub_id",
        "26050619","PubMed")

    fun restoreGraph() {
        if(Neo4jUtils.nodeExistsPredicate(pubmedNode)) {
            Neo4jUtils.deleteNodeById(pubmedNode)
            Neo4jUtils.deleteNodeById(parentNode)
        }
        //  create a dummy Parent node for testing
        val cypher = "MERGE (p:${parentNode.primaryLabel}{${parentNode.idProperty}: " +
                "${parentNode.idValue.formatNeo4jPropertyValue() }) return p"
        Neo4jConnectionService.executeCypherCommand(cypher)
    }

    fun loadPubMedNode(){
        PubMedNodeLoader().loadPubMedNode(parentNode,pubmedNode)
        LogService.info("Pub id 26050619 exists: " +
                "${PubMedPublicationDao.pubmedNodeExistsPredicate("26050619")}" +
                " (should be true)")
    }

    fun tearDown() {
        Neo4jUtils.deleteNodeById(pubmedNode)
        Neo4jUtils.deleteNodeById(pubmedNode)
        LogService.info("Neo4j test nodes deleted")
    }
}

fun main(){
    val test = TestPubMedLoader()
    test.restoreGraph()
    test.loadPubMedNode()
   // test.tearDown()

}