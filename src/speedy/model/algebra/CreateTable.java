package speedy.model.algebra;

import speedy.model.algebra.operators.IAlgebraTreeVisitor;
import speedy.model.algebra.operators.ITupleIterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.database.AttributeRef;
import speedy.model.database.IDatabase;

public class CreateTable extends AbstractOperator {

    private static Logger logger = LoggerFactory.getLogger(CreateTable.class);

    private String tableName;
    private String tableAlias;
    private String schemaName;
    private boolean withOIDs;

    public CreateTable(String tableName, String tableAlias, String schemaName, boolean withOIDs) {
        this.tableName = tableName;
        this.tableAlias = tableAlias;
        this.schemaName = schemaName;
        this.withOIDs = withOIDs;
    }

    public void accept(IAlgebraTreeVisitor visitor) {
        visitor.visitCreateTable(this);
    }

    public String getName() {
        return "CREATE TABLE " + tableName + " AS ";
    }

    public ITupleIterator execute(IDatabase source, IDatabase target) {
        return this.getChildren().get(0).execute(source, target);
    }

    public List<AttributeRef> getAttributes(IDatabase source, IDatabase target) {
        return this.getChildren().get(0).getAttributes(source, target);
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public boolean isWithOIDs() {
        return withOIDs;
    }

    public void setWithOIDs(boolean withOIDs) {
        this.withOIDs = withOIDs;
    }
}
