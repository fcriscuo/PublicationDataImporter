package org.batteryparkdev.placeholder.poc

import org.batteryparkdev.genomicgraphcore.common.service.LogService
import org.batteryparkdev.genomicgraphcore.neo4j.service.Neo4jConnectionService


/*
Basic integration test to confirm a valid connection to Neo4j
 */
fun main(){
    val cypher = "MATCH (n) RETURN COUNT (n)"
    val count = Neo4jConnectionService.executeCypherCommand(cypher)
    LogService.info("Node count $count")
}