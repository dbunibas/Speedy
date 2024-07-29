package speedy.model.expressions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.database.AttributeRef;
import speedy.model.database.TableAlias;

public class ExpressionAttribute extends AttributeRef {

    private static final Logger logger = LoggerFactory.getLogger(ExpressionAttribute.class);

    private Expression expression;

    public ExpressionAttribute(Expression expression, TableAlias tableAlias, String name) {
        super(tableAlias, name);
        this.expression = expression;
    }

    public ExpressionAttribute(Expression expression, String tableName, String name) {
        super(tableName, name);
        this.expression = expression;
    }

    public ExpressionAttribute(Expression expression, AttributeRef originalRef, TableAlias newAlias) {
        super(originalRef, newAlias);
        this.expression = expression;
    }

    public Expression getExpression() {
        return expression;
    }
}
