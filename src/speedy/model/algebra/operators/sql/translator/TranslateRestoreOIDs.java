package speedy.model.algebra.operators.sql.translator;

import speedy.model.algebra.IAlgebraOperator;
import speedy.model.algebra.RestoreOIDs;

public class TranslateRestoreOIDs {

    public void translate(RestoreOIDs operator, AlgebraTreeToSQLVisitor visitor) {
        IAlgebraOperator child = operator.getChildren().get(0);
        child.accept(visitor);
    }

}
