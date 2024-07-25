package speedy.model.algebra.aggregatefunctions;

import speedy.model.database.AttributeRef;
import speedy.model.database.IDatabase;
import speedy.model.database.IValue;
import speedy.model.database.Tuple;
import java.util.List;

public interface IAggregateFunction extends Cloneable{

    IValue evaluate(IDatabase db, List<Tuple> tuples);

    String getName();

    AttributeRef getAttributeRef();

    void setAttributeRef(AttributeRef attributeRef);
    
    IAggregateFunction clone();

}
