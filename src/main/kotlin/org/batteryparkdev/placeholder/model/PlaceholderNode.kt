package org.batteryparkdev.placeholder.model

/*
Represents the properties needed to create a child placeholder node,
its parent node, and the relationship between them
A placeholder node allows for the initial creation of a node as soon
as its identity occurs in the input data stream. The remaining properties
for that node can be completed by a subsequent or asynchronous task.
A specified property in the child node must be blank to identify it as a placeholder
node.
 */
data class PlaceholderNode(
    val parentNode: NodeIdentifier,
    val childNode: NodeIdentifier,
    val relationshipType: String,
    val blankPropertyType: String
) {
    fun isValid():Boolean =
        parentNode.isValid().and(childNode.isValid()).and(relationshipType.isNotBlank())
            .and(blankPropertyType.isNotBlank())
}

data class NodeIdentifier(
    val primaryLabel: String,
    val idProperty: String,
    val idValue: String,
    val secondaryLabel:String="",
){
    fun isValid():Boolean =
        primaryLabel.isNotBlank().and(idProperty.isNotBlank()).and(idValue.isNotBlank())
}