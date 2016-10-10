package speedy.model.algebra.operators.sql.translator;

import speedy.model.algebra.CartesianProduct;
import speedy.model.algebra.IAlgebraOperator;
import speedy.model.database.IDatabase;
import speedy.utility.SpeedyUtility;

public class TranslateCartesianProduct {

    public void translate(CartesianProduct operator, AlgebraTreeToSQLVisitor visitor) {
        SQLQueryBuilder result = visitor.getSQLQueryBuilder();
        result.append("SELECT * FROM ");
        for (IAlgebraOperator child : operator.getChildren()) {
            visitor.generateNestedSelect(child);
            result.append(", ");
        }
        SpeedyUtility.removeChars(", ".length(), result.getStringBuilder());
    }

}
