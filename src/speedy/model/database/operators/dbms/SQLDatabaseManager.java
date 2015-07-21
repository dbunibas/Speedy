package speedy.model.database.operators.dbms;

import speedy.model.database.EmptyDB;
import speedy.model.database.IDatabase;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.database.operators.IDatabaseManager;
import speedy.persistence.relational.AccessConfiguration;
import speedy.utility.DBMSUtility;
import speedy.persistence.relational.QueryManager;

public class SQLDatabaseManager implements IDatabaseManager {

    public IDatabase cloneTarget(DBMSDB target, String suffix) {
        AccessConfiguration targetConfiguration = target.getAccessConfiguration();
        String originalTargetSchemaName = targetConfiguration.getSchemaName();
        String cloneTargetSchemaName = originalTargetSchemaName + suffix;
        cloneSchema(originalTargetSchemaName, cloneTargetSchemaName, targetConfiguration);
//        AccessConfiguration workConfiguration = DBMSUtility.getWorkAccessConfiguration(targetConfiguration);
//        String originalWorkSchema = workConfiguration.getSchemaName();
//        String cloneWorkSchema = originalWorkSchema + suffix;
//        cloneSchema(originalWorkSchema, cloneWorkSchema, workConfiguration);
        AccessConfiguration cloneAccessConfiguration = targetConfiguration.clone();
        cloneAccessConfiguration.setSchemaName(cloneTargetSchemaName);
//        DBMSDB clone = new DBMSDB(target, cloneAccessConfiguration); //shallow copy
        DBMSDB clone = new DBMSDB(cloneAccessConfiguration);
        return clone;
    }

    public void removeClone(DBMSDB target, String suffix) {
        AccessConfiguration targetConfiguration = target.getAccessConfiguration();
        String originalTargetSchemaName = targetConfiguration.getSchemaName();
        String cloneTargetSchemaName = originalTargetSchemaName + suffix;
        removeSchema(cloneTargetSchemaName, targetConfiguration);
//        AccessConfiguration workConfiguration = DBMSUtility.getWorkAccessConfiguration(targetConfiguration);
//        String originalWorkSchema = workConfiguration.getSchemaName();
//        String cloneWorkSchema = originalWorkSchema + suffix;
//        removeSchema(cloneWorkSchema, targetConfiguration);
    }

    public void removeTable(String tableName, IDatabase db) {
        AccessConfiguration ac = ((DBMSDB) db).getAccessConfiguration();
        StringBuilder script = new StringBuilder();
        script.append("DROP TABLE ").append(ac.getSchemaName()).append(".").append(tableName).append("\n");
        QueryManager.executeScript(script.toString(), ac, true, true, true, false);
    }

    private void cloneSchema(String src, String dest, AccessConfiguration ac) {
        StringBuilder script = new StringBuilder();
        script.append(getCloneFunction()).append("\n");
        script.append("SELECT clone_schema('").append(src).append("','").append(dest).append("');");
        QueryManager.executeScript(script.toString(), ac, true, true, true, false);
    }

    private void removeSchema(String schema, AccessConfiguration ac) {
        String function = "drop schema " + schema + " cascade;";
        QueryManager.executeScript(function, ac, true, true, false, false);
    }

    private String getCloneFunction() {
        StringBuilder function = new StringBuilder();
        function.append("CREATE OR REPLACE FUNCTION clone_schema(source_schema text, dest_schema text) RETURNS void AS").append("\n");
        function.append("$BODY$").append("\n");
        function.append("DECLARE ").append("\n");
        function.append("  objeto text;").append("\n");
        function.append("  buffer text;").append("\n");
        function.append("BEGIN").append("\n");
        function.append("    EXECUTE 'CREATE SCHEMA ' || dest_schema ;").append("\n");
        function.append("    FOR objeto IN").append("\n");
        function.append("        SELECT table_name::text FROM information_schema.TABLES WHERE table_schema = source_schema").append("\n");
        function.append("    LOOP").append("\n");
        function.append("        buffer := dest_schema || '.' || objeto;").append("\n");
        function.append("        EXECUTE 'CREATE TABLE ' || buffer || ' (LIKE ' || source_schema || '.' || objeto || ' INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING DEFAULTS)';").append("\n");
//        function.append("        EXECUTE 'CREATE TABLE ' || buffer || ' (LIKE ' || source_schema || '.' || objeto || ' INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING DEFAULTS) WITH OIDS';").append("\n");
        function.append("        EXECUTE 'INSERT INTO ' || buffer || '(SELECT * FROM ' || source_schema || '.' || objeto || ')';").append("\n");
        function.append("    END LOOP;").append("\n");
        function.append("END;").append("\n");
        function.append("$BODY$").append("\n");
        function.append("LANGUAGE plpgsql VOLATILE;").append("\n");
        return function.toString();
    }

    public void analyzeDatabase(IDatabase db) {
        if (db == null || (db instanceof EmptyDB)) {
            return;
        }
        AccessConfiguration ac = ((DBMSDB) db).getAccessConfiguration();
        StringBuilder sb = new StringBuilder();
        for (String tableName : db.getTableNames()) {
            sb.append("VACUUM ANALYZE ").append(DBMSUtility.getSchema(ac)).append(tableName).append(";");
        }
        QueryManager.executeScript(sb.toString(), ac, true, true, false, false);
    }
}
