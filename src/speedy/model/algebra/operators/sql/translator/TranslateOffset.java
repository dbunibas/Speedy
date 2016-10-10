package speedy.model.algebra.operators.sql.translator;

import speedy.model.algebra.IAlgebraOperator;
import speedy.model.algebra.Offset;

public class TranslateOffset {

    public void translate(Offset operator, AlgebraTreeToSQLVisitor visitor) {
        SQLQueryBuilder result = visitor.getSQLQueryBuilder();
        IAlgebraOperator child = operator.getChildren().get(0);
        child.accept(visitor);
        result.append("\n").append(visitor.indentString());
        result.append("OFFSET ").append(operator.getOffset());
        result.append("\n");
    }

}
