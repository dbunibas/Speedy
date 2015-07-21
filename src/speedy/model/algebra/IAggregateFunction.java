package speedy.model.algebra;

import speedy.model.database.AttributeRef;
import speedy.model.database.IValue;
import speedy.model.database.Tuple;
import java.util.List;

public interface IAggregateFunction {
    
    public IValue evaluate(List<Tuple> tuples);
    
    public String getName();
    
    public AttributeRef getAttributeRef();

}
