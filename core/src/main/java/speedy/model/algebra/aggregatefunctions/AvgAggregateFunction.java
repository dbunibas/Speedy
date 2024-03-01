package speedy.model.algebra.aggregatefunctions;

import speedy.SpeedyConstants;
import speedy.model.database.AttributeRef;
import speedy.model.database.IValue;
import speedy.model.database.Tuple;
import speedy.model.database.NullValue;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import speedy.model.database.ConstantValue;

public class AvgAggregateFunction implements IAggregateFunction {

    private AttributeRef attributeRef;

    public AvgAggregateFunction(AttributeRef attributeRef) {
        this.attributeRef = attributeRef;
    }

    public IValue evaluate(List<Tuple> tuples) {
        if (tuples.isEmpty()) {
            return new NullValue(SpeedyConstants.NULL_VALUE);
        }
        double sum = 0;
        long count = 0;
        for (Tuple tuple : tuples) {
            IValue value = tuple.getCell(attributeRef).getValue();
            try {
                double doubleValue = Double.parseDouble(value.toString());
                sum += doubleValue;
                count++;
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("Unable to compute average on non-numerical value " + value);
            }
        }
        double avg = sum / (double) count;
        return new ConstantValue(avg + "");
    }

    public String getName() {
        return "avg";
    }

    public String toString() {
        return "avg(" + attributeRef + ") as " + attributeRef.getName();
    }

    public AttributeRef getAttributeRef() {
        return attributeRef;
    }

    public void setAttributeRef(AttributeRef attributeRef) {
        this.attributeRef = attributeRef;
    }

    public AvgAggregateFunction clone() {
        try {
            AvgAggregateFunction clone = (AvgAggregateFunction) super.clone();
            clone.attributeRef = this.attributeRef.clone();
            return clone;
        } catch (CloneNotSupportedException ex) {
            throw new IllegalArgumentException("Unable to clone " + ex.getLocalizedMessage());
        }
    }

}
