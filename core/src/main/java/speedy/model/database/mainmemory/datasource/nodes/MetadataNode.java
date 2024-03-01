package speedy.model.database.mainmemory.datasource.nodes;

import speedy.model.database.mainmemory.datasource.operators.INodeVisitor;

public class MetadataNode extends AttributeNode {
    
    public MetadataNode(String label) {
        super(label);
    }
    
    public MetadataNode(String label, Object value) {
        super(label, value);
    }
    
    public void accept(INodeVisitor visitor) {
        visitor.visitMetadataNode(this);
    }
    
}
