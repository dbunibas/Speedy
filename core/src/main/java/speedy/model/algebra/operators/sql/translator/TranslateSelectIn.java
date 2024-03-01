package speedy.model.algebra.operators.sql.translator;

import speedy.model.algebra.IAlgebraOperator;
import speedy.model.algebra.Join;
import speedy.model.algebra.Select;
import speedy.model.algebra.SelectIn;
import speedy.model.database.AttributeRef;
import speedy.model.database.IDatabase;
import speedy.utility.DBMSUtility;
import speedy.utility.SpeedyUtility;

public class TranslateSelectIn {

    public void translate(SelectIn operator, AlgebraTreeToSQLVisitor visitor) {
        SQLQueryBuilder result = visitor.getSQLQueryBuilder();
        IDatabase source = visitor.getSource();
        IDatabase target = visitor.getTarget();
        visitor.visitChildren(operator);
        result.append("\n").append(visitor.indentString());
        if (operator.getChildren() != null
                && (operator.getChildren().get(0) instanceof Select
                || operator.getChildren().get(0) instanceof Join)) {
            result.append(" AND ");
        } else {
            result.append(" WHERE ");
        }
//            result.append(" WHERE (");
        for (IAlgebraOperator selectionOperator : operator.getSelectionOperators()) {
            result.append("(");
            for (AttributeRef attributeRef : operator.getAttributes(source, target)) {
                result.append(DBMSUtility.attributeRefToSQLDot(attributeRef)).append(", ");
            }
            SpeedyUtility.removeChars(", ".length(), result.getStringBuilder());
            result.append(") IN (");
            result.append("\n").append(visitor.indentString());
            visitor.incrementIndentLevel();
            selectionOperator.accept(visitor);
            visitor.reduceIndentLevel();
            result.append("\n").append(visitor.indentString());
            result.append(")");
            result.append(" AND ");
        }
        SpeedyUtility.removeChars(" AND ".length(), result.getStringBuilder());
    }
}
