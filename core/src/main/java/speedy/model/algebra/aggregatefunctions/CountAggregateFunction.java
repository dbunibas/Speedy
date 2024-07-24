package speedy.model.algebra.aggregatefunctions;

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
        return "count(" + attributeRef + ")";
    }

    public AttributeRef getAttributeRef() {
        return attributeRef;
    }

    public void setAttributeRef(AttributeRef attributeRef) {
        this.attributeRef = attributeRef;
    }

    public CountAggregateFunction clone() {
        try {
            CountAggregateFunction clone = (CountAggregateFunction) super.clone();
            clone.attributeRef = this.attributeRef.clone();
            return clone;
        } catch (CloneNotSupportedException ex) {
            throw new IllegalArgumentException("Unable to clone " + ex.getLocalizedMessage());
        }
    }
}
