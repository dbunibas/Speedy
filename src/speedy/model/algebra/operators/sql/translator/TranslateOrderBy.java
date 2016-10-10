package speedy.model.algebra.operators.sql.translator;

import java.util.List;
import speedy.model.algebra.IAlgebraOperator;
import speedy.model.algebra.OrderBy;
import speedy.model.database.AttributeRef;
import speedy.model.database.IDatabase;
import speedy.utility.DBMSUtility;
import speedy.utility.SpeedyUtility;

public class TranslateOrderBy {

    public void translate(OrderBy operator, AlgebraTreeToSQLVisitor visitor) {
        SQLQueryBuilder result = visitor.getSQLQueryBuilder();
        IDatabase source = visitor.getSource();
        IDatabase target = visitor.getTarget();
        IAlgebraOperator child = operator.getChildren().get(0);
        child.accept(visitor);
        result.append("\n").append(visitor.indentString());
        result.append("ORDER BY ");
        for (AttributeRef attributeRef : operator.getAttributes(source, target)) {
            AttributeRef matchingAttribute = findFirstMatchingAttribute(attributeRef, visitor.getCurrentProjectionAttribute());
            result.append(DBMSUtility.attributeRefToSQL(matchingAttribute)).append(", ");
        }
        SpeedyUtility.removeChars(", ".length(), result.getStringBuilder());
        if (OrderBy.ORDER_DESC.equals(operator.getOrder())) {
            result.append(" ").append(OrderBy.ORDER_DESC);
        }
        result.append("\n");
    }

    private AttributeRef findFirstMatchingAttribute(AttributeRef originalAttribute, List<AttributeRef> attributes) {
        for (AttributeRef attribute : attributes) {
            if (attribute.getTableName().equalsIgnoreCase(originalAttribute.getTableName()) && attribute.getName().equalsIgnoreCase(originalAttribute.getName())) {
                return attribute;
            }
        }
        throw new IllegalArgumentException("Unable to find attribute " + originalAttribute + " into " + attributes);
    }

}
