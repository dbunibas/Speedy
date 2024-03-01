package speedy.model.algebra.aggregatefunctions;

import speedy.SpeedyConstants;
import speedy.model.database.AttributeRef;
import speedy.model.database.IValue;
import speedy.model.database.Tuple;
import speedy.model.database.NullValue;
import java.util.Collections;
import java.util.List;
import speedy.utility.comparator.TupleComparatorAttributeValue;

public class MinAggregateFunction implements IAggregateFunction {

    private AttributeRef attributeRef;

    public MinAggregateFunction(AttributeRef attributeRef) {
        this.attributeRef = attributeRef;
    }

    public IValue evaluate(List<Tuple> tuples) {
        if (tuples.isEmpty()) {
            return new NullValue(SpeedyConstants.NULL_VALUE);
        }
        Collections.sort(tuples, new TupleComparatorAttributeValue(attributeRef));
        Collections.reverse(tuples);
        return tuples.get(0).getCell(attributeRef).getValue();
    }

    public String getName() {
        return "min";
    }

    public String toString() {
        return "min(" + attributeRef + ") as " + attributeRef.getName();
    }

    public AttributeRef getAttributeRef() {
        return attributeRef;
    }

    public void setAttributeRef(AttributeRef attributeRef) {
        this.attributeRef = attributeRef;
    }

    public MinAggregateFunction clone() {
        try {
            MinAggregateFunction clone = (MinAggregateFunction) super.clone();
            clone.attributeRef = this.attributeRef.clone();
            return clone;
        } catch (CloneNotSupportedException ex) {
            throw new IllegalArgumentException("Unable to clone " + ex.getLocalizedMessage());
        }
    }

}
