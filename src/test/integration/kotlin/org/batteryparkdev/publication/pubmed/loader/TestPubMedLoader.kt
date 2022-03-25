package org.batteryparkdev.publication.pubmed.loader

import org.batteryparkdev.logging.service.LogService
import org.batteryparkdev.neo4j.service.Neo4jConnectionService
import org.batteryparkdev.neo4j.service.Neo4jUtils
import org.batteryparkdev.nodeidentifier.model.NodeIdentifier
import org.batteryparkdev.publication.pubmed.dao.PubMedPublicationDao

class TestPubMedLoader {
   private  val parentNode = NodeIdentifier("Parent","parent_id", "Parent123" )
    private val pubmedNode = NodeIdentifier("Publication", "pub_id",
        "26050619","PubMed")

    fun restoreGraph() {
        if(Neo4jUtils.nodeExistsPredicate(pubmedNode)) {
            Neo4jUtils.deleteNodeById(pubmedNode)
        }
        //  create a dummy Parent node for testing
        val cypher = "MERGE (p:${parentNode.primaryLabel}{${parentNode.idProperty}: " +
                "${Neo4jUtils.formatPropertyValue(parentNode.idValue)} }) return p"
        Neo4jConnectionService.executeCypherCommand(cypher)
    }

    fun loadPubMedNode(){
        PubMedNodeLoader().loadPubMedNode(parentNode,pubmedNode)
        LogService.logInfo("Pub id 26050619 exists: " +
                "${PubMedPublicationDao.publicationNodeExistsPredicate("26050619")}" +
                " (should be true)")
    }

    fun tearDown() {
        Neo4jUtils.deleteNodeById(pubmedNode)
        Neo4jUtils.deleteNodeById(pubmedNode)
        LogService.logInfo("Neo4j test nodes deleted")
    }
}

fun main(){
    val test = TestPubMedLoader()
    test.restoreGraph()
    test.loadPubMedNode()
    test.tearDown()

}