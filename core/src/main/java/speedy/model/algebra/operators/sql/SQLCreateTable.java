package speedy.model.algebra.operators.sql;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

import static java.lang.Boolean.TRUE;

public class SQLCreateTable implements ICreateTable {

    public void createTable(String tableName, List<Attribute> attributes, IDatabase target) {
        createTable(tableName, attributes, null, target);
    }

    public void createTable(String tableName, List<Attribute> attributes, Set<String> primaryKeys, IDatabase target) {
        AccessConfiguration accessConfiguration = ((DBMSDB) target).getAccessConfiguration();
        StringBuilder sb = new StringBuilder();
        sb.append("create table ").append(DBMSUtility.getSchemaNameAndDot(accessConfiguration)).append(tableName).append("(\n");
        if (!containsOID(attributes)) {
            sb.append(SpeedyConstants.INDENT).append(SpeedyConstants.OID).append(" serial,\n");
        }
        for (Attribute attribute : attributes) {
            String attributeName = attribute.getName();
            String attributeType = attribute.getType();
            sb.append(SpeedyConstants.INDENT)
                    .append(attributeName)
                    .append(" ")
                    .append(DBMSUtility.convertDataSourceTypeToDBType(attributeType))
                    .append(TRUE.equals(attribute.getNullable()) ? "" : " NOT NULL" )
                    .append(",\n");
        }
        SpeedyUtility.removeChars(",\n".length(), sb);
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            sb.append(", PRIMARY KEY(").append(String.join(", ", primaryKeys)).append(")");
        }
//        sb.append(") with oids;");
        sb.append(");");
        QueryManager.executeScript(sb.toString(), accessConfiguration, true, true, false, false);
        DBMSTable table = new DBMSTable(tableName, accessConfiguration);
        ((DBMSDB) target).addTable(table);
    }


    private boolean containsOID(List<Attribute> attributes) {
        for (Attribute attribute : attributes) {
            if (attribute.getName().equalsIgnoreCase(SpeedyConstants.OID)) {
                return true;
            }
        }
        return false;
    }
}
