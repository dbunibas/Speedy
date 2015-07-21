package speedy.model.algebra;

import speedy.model.database.AttributeRef;
import speedy.model.database.ConstantValue;
import speedy.model.database.IValue;
import speedy.model.database.Tuple;
import java.util.List;

public class CountAggregateFunction implements IAggregateFunction {

    private AttributeRef attributeRef;

    public CountAggregateFunction(AttributeRef attributeRef) {
        this.attributeRef = attributeRef;
    }

    public IValue evaluate(List<Tuple> tuples) {
        return new ConstantValue(tuples.size());
    }

    public String getName() {
        return "count";
    }

    public String toString() {
        return "count(*)";
    }

    public AttributeRef getAttributeRef() {
        return attributeRef;
    }
}
