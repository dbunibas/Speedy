package speedy.model.algebra.operators.sql.translator;

import speedy.model.algebra.IAlgebraOperator;
import speedy.model.algebra.Limit;
import speedy.model.algebra.Offset;
import speedy.model.algebra.OrderBy;
import speedy.model.algebra.Select;

public class TranslateSelect {

    public void translate(Select operator, AlgebraTreeToSQLVisitor visitor) {
        SQLQueryBuilder result = visitor.getSQLQueryBuilder();
        IAlgebraOperator child = operator.getChildren().get(0);
        if (child instanceof OrderBy || child instanceof Offset || child instanceof Limit) {
            result.append("SELECT * FROM ");
            visitor.generateNestedSelect(child);
        } else {
            visitor.visitChildren(operator);
        }
        visitor.createWhereClause(operator, false);
    }

}
