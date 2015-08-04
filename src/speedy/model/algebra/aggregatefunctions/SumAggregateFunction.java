package speedy.model.algebra.aggregatefunctions;

import speedy.SpeedyConstants;
import speedy.model.database.AttributeRef;
import speedy.model.database.IValue;
import speedy.model.database.Tuple;
import speedy.model.database.NullValue;
import java.util.List;
import speedy.model.database.ConstantValue;

public class SumAggregateFunction implements IAggregateFunction {

    private AttributeRef attributeRef;

    public SumAggregateFunction(AttributeRef attributeRef) {
        this.attributeRef = attributeRef;
    }

    public IValue evaluate(List<Tuple> tuples) {
        if (tuples.isEmpty()) {
            return new NullValue(SpeedyConstants.NULL_VALUE);
        }
        double sum = 0;
        for (Tuple tuple : tuples) {
            IValue value = tuple.getCell(attributeRef).getValue();
            try {
                double doubleValue = Double.parseDouble(value.toString());
                sum += doubleValue;
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("Unable to compute sum on non-numerical value " + value);
            }
        }
        return new ConstantValue(sum + "");
    }

    public String getName() {
        return "sum";
    }

    public String toString() {
        return "sum(" + attributeRef + ") as " + attributeRef.getName();
    }

    public AttributeRef getAttributeRef() {
        return attributeRef;
    }

}
