package speedy.model.database.mainmemory.datasource.operators;

import speedy.model.database.mainmemory.datasource.nodes.AttributeNode;
import speedy.model.database.mainmemory.datasource.nodes.LeafNode;
import speedy.model.database.mainmemory.datasource.nodes.MetadataNode;
import speedy.model.database.mainmemory.datasource.nodes.SequenceNode;
import speedy.model.database.mainmemory.datasource.nodes.SetNode;
import speedy.model.database.mainmemory.datasource.nodes.TupleNode;

public interface INodeVisitor {
        
    void visitSetNode(SetNode node);
    
    void visitTupleNode(TupleNode node);
    
    void visitSequenceNode(SequenceNode node);
    
    void visitAttributeNode(AttributeNode node);
    
    void visitMetadataNode(MetadataNode node);
        
    void visitLeafNode(LeafNode node);

    Object getResult();
    
}
