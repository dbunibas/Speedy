package speedy.model.algebra.operators.sql;

import speedy.utility.SpeedyUtility;
import speedy.model.algebra.operators.IInsertTuple;
import speedy.model.database.dbms.DBMSTable;
import speedy.persistence.Types;
import speedy.persistence.relational.AccessConfiguration;
import speedy.persistence.relational.QueryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.database.Attribute;
import speedy.model.database.AttributeRef;
import speedy.model.database.Cell;
import speedy.model.database.IDatabase;
import speedy.model.database.ITable;
import speedy.model.database.Tuple;

public class SQLInsertTuple implements IInsertTuple {

    private static Logger logger = LoggerFactory.getLogger(SQLInsertTuple.class);

    public void execute(ITable table, Tuple tuple, IDatabase source, IDatabase target) {
        DBMSTable dbmsTable = (DBMSTable) table;
        StringBuilder insertQuery = buildInsertScript(dbmsTable, tuple, source, target);
        if (logger.isDebugEnabled()) logger.debug("Insert query:\n" + insertQuery.toString());
        QueryManager.executeInsertOrDelete(insertQuery.toString(), ((DBMSTable) table).getAccessConfiguration());
    }

    public StringBuilder buildInsertScript(DBMSTable dbmsTable, Tuple tuple, IDatabase source, IDatabase target) {
        AccessConfiguration accessConfiguration = dbmsTable.getAccessConfiguration();
        StringBuilder insertQuery = new StringBuilder();
        insertQuery.append("INSERT INTO ");
        insertQuery.append(accessConfiguration.getSchemaName()).append(".").append(dbmsTable.getName());
        insertQuery.append(" (");
        for (Cell cell : tuple.getCells()) {
            insertQuery.append(cell.getAttribute()).append(", ");
        }
        SpeedyUtility.removeChars(", ".length(), insertQuery);
        insertQuery.append(")");
        insertQuery.append(" VALUES (");
        for (Cell cell : tuple.getCells()) {
            String cellValue = cell.getValue().toString();
            cellValue = cellValue.replaceAll("'", "''");
            String attributeType = getAttributeType(cell.getAttributeRef(), source, target);
            if (attributeType.equals(Types.STRING)) {
                insertQuery.append("'");
            }
            insertQuery.append(cellValue);
            if (attributeType.equals(Types.STRING)) {
                insertQuery.append("'");
            }
            insertQuery.append(", ");
        }
        SpeedyUtility.removeChars(", ".length(), insertQuery);
        insertQuery.append(");");
        return insertQuery;
    }

    private String getAttributeType(AttributeRef attributeRef, IDatabase source, IDatabase target) {
        ITable table = SpeedyUtility.getTable(attributeRef, source, target);
        for (Attribute attribute : table.getAttributes()) {
            if (attribute.getName().equals(attributeRef.getName())) {
                return attribute.getType();
            }
        }
        //Original table doesn't contain the attribute (delta db attribute)
        return Types.STRING;
    }
}
