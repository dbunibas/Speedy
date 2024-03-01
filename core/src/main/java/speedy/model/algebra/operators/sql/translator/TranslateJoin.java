package speedy.model.algebra.operators.sql.translator;

import java.util.List;
import speedy.model.algebra.IAlgebraOperator;
import speedy.model.algebra.Join;

public class TranslateJoin {

    public void translate(Join operator, AlgebraTreeToSQLVisitor visitor) {
        SQLQueryBuilder result = visitor.getSQLQueryBuilder();
        List<NestedOperator> nestedSelect = visitor.findNestedTablesForJoin(operator);
        visitor.createSQLSelectClause(operator, nestedSelect, true);
        result.append(" FROM ");
        IAlgebraOperator leftChild = operator.getChildren().get(0);
        IAlgebraOperator rightChild = operator.getChildren().get(1);
        visitor.createJoinClause(operator, leftChild, rightChild, nestedSelect);
    }

}
