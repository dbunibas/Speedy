package speedy.model.database.operators.dbms;

import speedy.model.database.EmptyDB;
import speedy.model.database.IDatabase;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.database.operators.IAnalyzeDatabase;
import speedy.persistence.relational.AccessConfiguration;
import speedy.persistence.relational.QueryManager;
import speedy.utility.DBMSUtility;

public class SQLAnalyzeDatabase implements IAnalyzeDatabase {

    public void analyze(IDatabase database) {
        if (database instanceof EmptyDB) {
            return;
        }
        DBMSDB dbmsDB = (DBMSDB) database;
        AccessConfiguration accessConfiguration = dbmsDB.getAccessConfiguration();
        StringBuilder sb = new StringBuilder();
        for (String tableName : database.getTableNames()) {
            sb.append("ANALYZE ").append(DBMSUtility.getSchemaNameAndDot(accessConfiguration)).append(tableName).append(";\n");
        }
        QueryManager.executeScript(sb.toString(), accessConfiguration, true, true, true, false);
    }

}
