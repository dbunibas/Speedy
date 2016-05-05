package speedy.model.algebra.aggregatefunctions;

import speedy.SpeedyConstants;
import speedy.model.database.AttributeRef;
import speedy.model.database.IValue;
import speedy.model.database.Tuple;
import speedy.model.database.NullValue;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import speedy.model.database.ConstantValue;

public class StdDevAggregateFunction implements IAggregateFunction {

    private AttributeRef attributeRef;

    public StdDevAggregateFunction(AttributeRef attributeRef) {
        this.attributeRef = attributeRef;
    }

    public IValue evaluate(List<Tuple> tuples) {
        if (tuples.isEmpty()) {
            return new NullValue(SpeedyConstants.NULL_VALUE);
        }
        SummaryStatistics stats = new SummaryStatistics();
        for (Tuple tuple : tuples) {
            IValue value = tuple.getCell(attributeRef).getValue();
            try {
                double doubleValue = Double.parseDouble(value.toString());
                stats.addValue(doubleValue);
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("Unable to compute average on non-numerical value " + value);
            }
        }
        return new ConstantValue(stats.getStandardDeviation());
    }

    public String getName() {
        return "stddev";
    }

    public String toString() {
        return "stddev(" + attributeRef + ") as " + attributeRef.getName();
    }

    public AttributeRef getAttributeRef() {
        return attributeRef;
    }

    public void setAttributeRef(AttributeRef attributeRef) {
        this.attributeRef = attributeRef;
    }

    public StdDevAggregateFunction clone() {
        try {
            StdDevAggregateFunction clone = (StdDevAggregateFunction) super.clone();
            clone.attributeRef = this.attributeRef.clone();
            return clone;
        } catch (CloneNotSupportedException ex) {
            throw new IllegalArgumentException("Unable to clone " + ex.getLocalizedMessage());
        }
    }

}
