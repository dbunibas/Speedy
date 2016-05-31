package speedy.model.database.operators.dbms;

import speedy.model.database.EmptyDB;
import speedy.model.database.IDatabase;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.database.operators.IAnalyzeDatabase;
import speedy.model.thread.IBackgroundThread;
import speedy.model.thread.ThreadManager;
import speedy.persistence.relational.AccessConfiguration;
import speedy.persistence.relational.QueryManager;
import speedy.utility.DBMSUtility;

public class SQLAnalyzeDatabase implements IAnalyzeDatabase {

    public void analyze(IDatabase database, int maxNumberOfThreads) {
        if (database instanceof EmptyDB) {
            return;
        }
        ThreadManager threadManager = new ThreadManager(maxNumberOfThreads);
        for (String tableName : database.getTableNames()) {
            DBMSDB dbmsDB = (DBMSDB) database;
            AnalyzeTableThread analyzeThread = new AnalyzeTableThread(tableName, dbmsDB);
            threadManager.startThread(analyzeThread);
        }
        threadManager.waitForActiveThread();
    }

    class AnalyzeTableThread implements IBackgroundThread {

        private String tableName;
        private DBMSDB dbmsDB;

        public AnalyzeTableThread(String tableName, DBMSDB dbmsDB) {
            this.tableName = tableName;
            this.dbmsDB = dbmsDB;
        }

        public void execute() {
            AccessConfiguration accessConfiguration = dbmsDB.getAccessConfiguration();
            StringBuilder sb = new StringBuilder();
            sb.append("ANALYZE ").append(DBMSUtility.getSchemaNameAndDot(accessConfiguration)).append(tableName).append(";\n");
            QueryManager.executeScript(sb.toString(), accessConfiguration, true, true, true, false);
        }

    }

}
