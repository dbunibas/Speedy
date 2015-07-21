package speedy.persistence.xml.model;

import speedy.persistence.xml.operators.IXSDNodeVisitor;

public class AttributeDeclaration extends Particle {
    
    public AttributeDeclaration(String label) {
        super(label);
    }

    public void accept(IXSDNodeVisitor visitor) {
        visitor.visitAttributeDeclaration(this);
    }

}
