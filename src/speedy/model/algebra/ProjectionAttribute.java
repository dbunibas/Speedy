package speedy.model.algebra;

import speedy.model.algebra.aggregatefunctions.IAggregateFunction;
import speedy.model.database.AttributeRef;

public class ProjectionAttribute {

    private AttributeRef attributeRef;
    private IAggregateFunction aggregateFunction;

    public ProjectionAttribute(AttributeRef attributeRef) {
        this.attributeRef = attributeRef;
    }

    public ProjectionAttribute(IAggregateFunction aggregateFunction) {
        this.aggregateFunction = aggregateFunction;
    }

    public AttributeRef getAttributeRef() {
        if (isAggregative()) {
            return aggregateFunction.getAttributeRef();
        } else {
            return attributeRef;
        }
    }

    public IAggregateFunction getAggregateFunction() {
        return aggregateFunction;
    }

    @Override
    public String toString() {
        if (isAggregative()) {
            return aggregateFunction.getAttributeRef().toString();
        } else {
            return attributeRef.toString();
        }
    }

    public boolean isAggregative() {
        return aggregateFunction != null;
    }
}
