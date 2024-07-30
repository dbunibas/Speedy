package speedy.model.expressions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.database.AttributeRef;
import speedy.model.database.TableAlias;
import speedy.model.database.VirtualAttributeRef;

public class ExpressionAttributeRef extends VirtualAttributeRef {

    private static final Logger logger = LoggerFactory.getLogger(ExpressionAttributeRef.class);

    private Expression expression;

    public ExpressionAttributeRef(Expression expression, TableAlias tableAlias, String name, String type) {
        super(tableAlias, name, type);
        this.expression = expression;
    }

    public ExpressionAttributeRef(Expression expression, String tableName, String name, String type) {
        super(tableName, name, type);
        this.expression = expression;
    }

    public ExpressionAttributeRef(Expression expression, AttributeRef originalRef, TableAlias newAlias, String type) {
        super(originalRef, newAlias, type);
        this.expression = expression;
    }

    public Expression getExpression() {
        return expression;
    }
}
