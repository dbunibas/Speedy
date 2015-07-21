package speedy.persistence.xml.operators;

import speedy.persistence.xml.model.AttributeDeclaration;
import speedy.persistence.xml.model.ElementDeclaration;
import speedy.persistence.xml.model.PCDATA;
import speedy.persistence.xml.model.SimpleType;
import speedy.persistence.xml.model.TypeCompositor;

public interface IXSDNodeVisitor {
        
    void visitSimpleType(SimpleType node);
    
    void visitElementDeclaration(ElementDeclaration node);
    
    void visitTypeCompositor(TypeCompositor node);
    
    void visitAttributeDeclaration(AttributeDeclaration node);
    
    void visitPCDATA(PCDATA node);
    
    Object getResult();
    
}
