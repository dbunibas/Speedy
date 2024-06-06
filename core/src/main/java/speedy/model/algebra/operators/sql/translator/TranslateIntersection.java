package speedy.model.algebra.operators.sql.translator;

import speedy.model.algebra.IAlgebraOperator;
import speedy.model.algebra.Intersection;

public class TranslateIntersection {

    public void translate(Intersection operator, AlgebraTreeToSQLVisitor visitor) {
        SQLQueryBuilder result = visitor.getSQLQueryBuilder();
        IAlgebraOperator leftChild = operator.getChildren().get(0);
        leftChild.accept(visitor);
        result.append("\n").append(visitor.indentString());
        result.append(" INTERSECT \n");
        IAlgebraOperator rightChild = operator.getChildren().get(1);
        visitor.incrementIndentLevel();
        rightChild.accept(visitor);
        visitor.reduceIndentLevel();
    }

}
