package speedy.model.algebra.operators.sql.translator;

import java.util.List;
import speedy.model.algebra.CartesianProduct;
import speedy.model.algebra.IAlgebraOperator;
import speedy.model.database.AttributeRef;
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
        List<AttributeRef> attributes = operator.getAttributes(visitor.getSource(), visitor.getTarget());
        visitor.setCurrentProjectionAttribute(attributes);
        SpeedyUtility.removeChars(", ".length(), result.getStringBuilder());
    }

}
