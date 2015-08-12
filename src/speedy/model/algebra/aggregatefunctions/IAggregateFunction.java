package speedy.model.algebra.aggregatefunctions;

import speedy.model.database.AttributeRef;
import speedy.model.database.IValue;
import speedy.model.database.Tuple;
import java.util.List;

public interface IAggregateFunction extends Cloneable{

    public IValue evaluate(List<Tuple> tuples);

    public String getName();

    public AttributeRef getAttributeRef();

    public void setAttributeRef(AttributeRef attributeRef);
    
    public IAggregateFunction clone();

}
