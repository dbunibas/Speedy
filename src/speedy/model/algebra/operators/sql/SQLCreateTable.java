package speedy.model.algebra.operators.sql;

import java.util.List;
import speedy.SpeedyConstants;
import speedy.model.algebra.operators.ICreateTable;
import speedy.model.database.Attribute;
import speedy.model.database.IDatabase;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.database.dbms.DBMSTable;
import speedy.persistence.relational.AccessConfiguration;
import speedy.persistence.relational.QueryManager;
import speedy.utility.DBMSUtility;
import speedy.utility.SpeedyUtility;

public class SQLCreateTable implements ICreateTable {

    public void createTable(String tableName, List<Attribute> attributes, IDatabase target) {
        AccessConfiguration accessConfiguration = ((DBMSDB) target).getAccessConfiguration();
        StringBuilder sb = new StringBuilder();
        sb.append("create table ").append(DBMSUtility.getSchema(accessConfiguration)).append(tableName).append("(\n");
        sb.append(SpeedyConstants.INDENT).append("oid serial,\n");
        for (Attribute attribute : attributes) {
            String attributeName = attribute.getName();
            String attributeType = attribute.getType();
            sb.append(SpeedyConstants.INDENT).append(attributeName).append(" ").append(DBMSUtility.convertDataSourceTypeToDBType(attributeType)).append(",\n");
        }
        SpeedyUtility.removeChars(",\n".length(), sb);
//        sb.append(") with oids;");
        sb.append(");");
        QueryManager.executeScript(sb.toString(), accessConfiguration, true, true, false, false);
        DBMSTable table = new DBMSTable(tableName, accessConfiguration);
        ((DBMSDB) target).addTable(table);
    }
}
