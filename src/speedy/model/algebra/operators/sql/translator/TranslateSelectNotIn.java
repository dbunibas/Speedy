package speedy.model.algebra.operators.sql.translator;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.algebra.IAlgebraOperator;
import speedy.model.algebra.SelectNotIn;
import speedy.model.database.AttributeRef;
import speedy.model.database.IDatabase;
import speedy.utility.DBMSUtility;
import speedy.utility.SpeedyUtility;

public class TranslateSelectNotIn {

    private final static Logger logger = LoggerFactory.getLogger(TranslateSelectNotIn.class);

    public void translate(SelectNotIn operator, AlgebraTreeToSQLVisitor visitor) {
        if (visitor.getCurrentSelectNotIn() == null) {
            visitor.setCurrentSelectNotIn(operator);
            visitor.setSelectNotInBufferWhere(new StringBuilder());
        }
        SQLQueryBuilder result = visitor.getSQLQueryBuilder();
        IDatabase source = visitor.getSource();
        IDatabase target = visitor.getTarget();
        visitor.visitChildren(operator);
        result.append("\n").append(visitor.indentString());
        result.append(" LEFT JOIN ");
        IAlgebraOperator rightOperator = operator.getSelectionOperator();
        result.append("(");
        result.append("\n").append(visitor.indentString());
        rightOperator.accept(visitor);
        visitor.incrementIndentLevel();
        result.append("\n").append(visitor.indentString()).append(") AS ");
        result.append("Nest_").append(operator.hashCode());
        visitor.reduceIndentLevel();
        result.append("\n").append(visitor.indentString());
        result.append(" ON ");
        List<AttributeRef> leftAttributes = operator.getAttributes(source, target);
        List<AttributeRef> rightAttributes = rightOperator.getAttributes(source, target);
        assert (!leftAttributes.isEmpty());
        for (int i = 0; i < leftAttributes.size(); i++) {
            AttributeRef leftAttribute = leftAttributes.get(i);
            AttributeRef rightAttribute = rightAttributes.get(i);
            result.append(DBMSUtility.attributeRefToSQLDot(leftAttribute));
            result.append(" = ");
            result.append(DBMSUtility.attributeRefToAliasSQL(rightAttribute));
            result.append(" AND ");
        }
        SpeedyUtility.removeChars(" AND ".length(), result.getStringBuilder());
        result.append("\n").append(visitor.indentString());
        if (visitor.getSelectNotInBufferWhere().length() > 0) {
            visitor.getSelectNotInBufferWhere().append(" AND ");
        }
        for (int i = 0; i < rightAttributes.size(); i++) {
            AttributeRef rightAttribute = rightAttributes.get(i);
            visitor.getSelectNotInBufferWhere().append(DBMSUtility.attributeRefToAliasSQL(rightAttribute));
            visitor.getSelectNotInBufferWhere().append(" IS NULL ");
            visitor.getSelectNotInBufferWhere().append(" AND ");
        }
        SpeedyUtility.removeChars(" AND ".length(), visitor.getSelectNotInBufferWhere());
        if (visitor.getCurrentSelectNotIn() == operator) {
            result.append(" WHERE ");
            result.append("\n").append(visitor.indentString());
            result.append("\n").append(visitor.getSelectNotInBufferWhere());
            visitor.setCurrentSelectNotIn(null);
        }
    }
//    public void translate(SelectNotIn operator, AlgebraTreeToSQLVisitor visitor) {
//        SQLQueryBuilder result = visitor.getSQLQueryBuilder();
//        IDatabase source = visitor.getSource();
//        IDatabase target = visitor.getTarget();
//        visitor.visitChildren(operator);
//        result.append("\n").append(visitor.indentString());
//        if (operator.getChildren() != null
//                && (operator.getChildren().get(0) instanceof Select
//                || operator.getChildren().get(0) instanceof Join
//                || operator.getChildren().get(0) instanceof SelectNotIn)) {
//            result.append(" AND ");
//        } else {
//            result.append(" WHERE ");
//        }
//        result.append("(");
//        for (AttributeRef attributeRef : operator.getAttributes(source, target)) {
//            result.append(DBMSUtility.attributeRefToSQLDot(attributeRef)).append(", ");
//        }
//        SpeedyUtility.removeChars(", ".length(), result.getStringBuilder());
//        result.append(") NOT IN (");
//        result.append("\n").append(visitor.indentString());
//        visitor.incrementIndentLevel();
//        IAlgebraOperator selectionOperator = operator.getSelectionOperator();
//        selectionOperator.accept(visitor);
//        visitor.reduceIndentLevel();
//        result.append("\n").append(visitor.indentString());
//        result.append(")");
//        result.append(" AND ");
//        SpeedyUtility.removeChars(" AND ".length(), result.getStringBuilder());
//    }
}
