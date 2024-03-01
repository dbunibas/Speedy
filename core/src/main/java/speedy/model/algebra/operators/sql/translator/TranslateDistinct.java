package speedy.model.algebra.operators.sql.translator;

import speedy.model.algebra.Distinct;
import speedy.model.algebra.IAlgebraOperator;

public class TranslateDistinct {

    public void translate(Distinct operator, AlgebraTreeToSQLVisitor visitor) {
        SQLQueryBuilder result = visitor.getSQLQueryBuilder();
        result.setDistinct(true);
        IAlgebraOperator child = operator.getChildren().get(0);
        child.accept(visitor);
    }

}
