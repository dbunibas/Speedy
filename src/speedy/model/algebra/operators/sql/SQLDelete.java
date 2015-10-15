package speedy.model.algebra.operators.sql;

import speedy.utility.SpeedyUtility;
import speedy.model.algebra.IAlgebraOperator;
import speedy.model.algebra.Scan;
import speedy.model.algebra.Select;
import speedy.model.algebra.operators.IDelete;
import speedy.model.database.IDatabase;
import speedy.model.database.TableAlias;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.expressions.Expression;
import speedy.utility.DBMSUtility;
import speedy.persistence.relational.QueryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.persistence.relational.AccessConfiguration;

public class SQLDelete implements IDelete {

    private static Logger logger = LoggerFactory.getLogger(SQLDelete.class);

    public boolean execute(String tableName, IAlgebraOperator operator, IDatabase source, IDatabase target) {
        StringBuilder deleteQuery = new StringBuilder();
        deleteQuery.append("DELETE FROM ");
        if (operator == null) {
            deleteQuery.append(tableAliasToSQL(new TableAlias(tableName), source, target, ((DBMSDB) target).getAccessConfiguration()));
        } else {
            deleteQuery.append(getScanQuery(operator, source, target));
        }
        deleteQuery.append(getSelectQuery(operator));
        deleteQuery.append(";");
        if (logger.isDebugEnabled()) logger.debug("Delete query:\n" + deleteQuery.toString());
        return QueryManager.executeInsertOrDelete(deleteQuery.toString(), ((DBMSDB) target).getAccessConfiguration());
    }

    private String getScanQuery(IAlgebraOperator operator, IDatabase source, IDatabase target) {
        if (operator instanceof Scan) {
            TableAlias tableAlias = ((Scan) operator).getTableAlias();
            return tableAliasToSQL(tableAlias, source, target, ((DBMSDB) target).getAccessConfiguration());
        }
        for (IAlgebraOperator child : operator.getChildren()) {
            return getScanQuery(child, source, target);
        }
        throw new IllegalArgumentException("Unable to create delete query from " + operator);
    }

    private String getSelectQuery(IAlgebraOperator operator) {
        if (operator == null) {
            return "";
        }
        if (operator instanceof Select) {
            StringBuilder result = new StringBuilder();
            result.append(" WHERE ");
            for (Expression condition : ((Select) operator).getSelections()) {
                result.append(DBMSUtility.expressionToSQL(condition));
                result.append(" AND ");
            }
            SpeedyUtility.removeChars(" AND ".length(), result);
            return result.toString();
        }
        for (IAlgebraOperator child : operator.getChildren()) {
            return getSelectQuery(child);
        }
        return "";
    }

    private String tableAliasToSQL(TableAlias tableAlias, IDatabase source, IDatabase target, AccessConfiguration configuration) {
        StringBuilder sb = new StringBuilder();
        if (DBMSUtility.supportsSchema(configuration)) {
            String sourceSchemaName = "source";
            if (source != null && source instanceof DBMSDB) {
                sourceSchemaName = ((DBMSDB) source).getAccessConfiguration().getSchemaName();
            }
            String targetSchemaName = "target";
            if (target != null && target instanceof DBMSDB) {
                targetSchemaName = ((DBMSDB) target).getAccessConfiguration().getSchemaName();
            }
            if (tableAlias.isSource()) {
                sb.append(sourceSchemaName);
            } else {
                sb.append(targetSchemaName);
            }
            sb.append(".");
        }
        sb.append(tableAlias.getTableName());
        if (tableAlias.isAliased()) {
            sb.append(" AS ").append(DBMSUtility.tableAliasToSQL(tableAlias));
        }
        return sb.toString();
    }
}
