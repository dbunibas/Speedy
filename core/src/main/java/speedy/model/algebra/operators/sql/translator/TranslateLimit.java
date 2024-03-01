package speedy.model.algebra.operators.sql.translator;

import speedy.model.algebra.IAlgebraOperator;
import speedy.model.algebra.Limit;

public class TranslateLimit {

    public void translate(Limit operator, AlgebraTreeToSQLVisitor visitor) {
        SQLQueryBuilder result = visitor.getSQLQueryBuilder();
        IAlgebraOperator child = operator.getChildren().get(0);
        child.accept(visitor);
        result.append("\n").append(visitor.indentString());
        result.append("LIMIT ").append(operator.getSize());
        result.append("\n");
    }

}
