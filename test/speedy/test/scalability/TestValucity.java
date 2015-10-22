package speedy.test.scalability;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.Date;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.database.dbms.DBMSTupleIterator;
import speedy.model.database.dbms.InitDBConfiguration;
import speedy.persistence.DAODBMSDatabase;
import speedy.persistence.file.XMLFile;
import speedy.persistence.relational.QueryManager;
import speedy.test.utility.TestResults;
import speedy.test.utility.UtilityForTests;
import speedy.utility.Size;
import speedy.utility.SpeedyUtility;

public class TestValucity {

    private static Logger logger = LoggerFactory.getLogger(TestValucity.class);

    private String baseFolder = "/Users/donatello/Dropbox-Informatica/Shared Folders/valucity/scenario/";
    private boolean recreateDB = false;
    private String[] views = new String[]{
        "vista_servizi",
        "vista_date_valutazioni_attuali",
        "vista_valutazioni",
        "vista_luoghi",
        "vista_dati_servizi",
        "vista_dati_servizi_macro",
        "vista_valutatori_servizio",
        "vista_valutatori_servizio_macro",
        "vista_punteggi_servizi",
        "vista_punteggi_servizi_macro",
        "vista_punteggi_massimi_servizi_macro"
    };

    @Test
    public void runMultiple() {
        TestResults.resetResults();
//        executeQuery(Size.S_100K, Size.S_7K);
        executeQuery(Size.S_100K, Size.S_70K);
//        executeQuery(Size.S_500K, Size.S_7K);
//        executeQuery(Size.S_500K, Size.S_70K);
//        executeQuery(Size.S_1M, Size.S_7K);
//        executeQuery(Size.S_1M, Size.S_70K);
        TestResults.printResults("Test_Valucity");
    }

    private void executeQuery(Size sizeV, Size sizeL) {
        String sizeString = sizeV.toString() + "_" + sizeL.toString();
//        DBMSDB database = getDatabasePostgres(sizeV, sizeL);
//        String createViewScript = loadScript("script/viste.sql");
        DBMSDB database = getDatabaseMySQL(sizeV, sizeL);
        String createViewScript = loadScript("script/viste_mysql.sql");
        database.initDBMS();
        long createViewTime = new Date().getTime();
        QueryManager.executeScript(createViewScript, database.getAccessConfiguration(), true, true, false, false);
        long end = new Date().getTime();
        long executionTime = end - createViewTime;
        if (logger.isDebugEnabled()) logger.debug("Create views execution time: " + executionTime);
        TestResults.addTimeResult(sizeString, "CreateView", executionTime);
        for (String view : views) {
            runView(view, sizeString, database);
        }
        TestResults.printStats("\n****  Size: " + sizeString + "  ****");
    }

    private void runView(String view, String sizeString, DBMSDB database) {
        long createViewTime = new Date().getTime();
        ResultSet resultSet = QueryManager.executeQuery("SELECT * FROM " + view, database.getAccessConfiguration());
        ITupleIterator result = new DBMSTupleIterator(resultSet, view);
        long resultSize = SpeedyUtility.getTupleIteratorSize(result);
        if (logger.isDebugEnabled()) logger.debug("Result size: " + resultSize);
        long end = new Date().getTime();
        long executionTime = end - createViewTime;
        if (logger.isDebugEnabled()) logger.debug("View " + view + " execution time: " + executionTime);
        if (logger.isDebugEnabled()) logger.debug("View " + view + " size: " + resultSize);
        TestResults.addTimeResult(sizeString, "View " + view, executionTime);
        result.close();
    }

    private DBMSDB getDatabasePostgres(Size sizeV, Size sizeL) {
        DAODBMSDatabase daoDatabase = new DAODBMSDatabase();
        String driver = "org.postgresql.Driver";
        String uri = "jdbc:postgresql:speedy_valucity_" + sizeV.toString() + "v_" + sizeL.toString() + "l";
        String schema = "public";
        String login = "pguser";
        String password = "pguser";
        DBMSDB database = daoDatabase.loadDatabase(driver, uri, schema, login, password);
        InitDBConfiguration initDBConfiguration = database.getInitDBConfiguration();
        initDBConfiguration.setCreateTablesFromFiles(true);
        String datasetPath = baseFolder + "datasets/";
        String suffix = sizeV.toString() + "_" + sizeL.toString();
        initDBConfiguration.addFileToImportForTable("luogo", new XMLFile(datasetPath + "luogo.xml"));
        initDBConfiguration.addFileToImportForTable("servizio", new XMLFile(datasetPath + "servizio.xml"));
        initDBConfiguration.addFileToImportForTable("valutazione", new XMLFile(datasetPath + "valutazione_" + suffix + ".xml"));
        initDBConfiguration.setPostDBScript(loadScript("script/create_key_fk_postgres.sql"));
        if (recreateDB) UtilityForTests.deleteDB(database.getAccessConfiguration());
        return database;
    }

    private DBMSDB getDatabaseMySQL(Size sizeV, Size sizeL) {
        DAODBMSDatabase daoDatabase = new DAODBMSDatabase();
        String driver = "com.mysql.jdbc.Driver";
        String uri = "jdbc:mysql://localhost/speedy_valucity_" + sizeV.toString() + "v_" + sizeL.toString() + "l";
        String schema = "public";
        String login = "mysqluser";
        String password = "mysqluser";
        DBMSDB database = daoDatabase.loadDatabase(driver, uri, schema, login, password);
        InitDBConfiguration initDBConfiguration = database.getInitDBConfiguration();
        initDBConfiguration.setCreateTablesFromFiles(true);
        String datasetPath = baseFolder + "datasets/";
        String suffix = sizeV.toString() + "_" + sizeL.toString();
        initDBConfiguration.addFileToImportForTable("luogo", new XMLFile(datasetPath + "luogo.xml"));
        initDBConfiguration.addFileToImportForTable("servizio", new XMLFile(datasetPath + "servizio.xml"));
        initDBConfiguration.addFileToImportForTable("valutazione", new XMLFile(datasetPath + "valutazione_" + suffix + ".xml"));
        initDBConfiguration.setPostDBScript(loadScript("script/create_key_fk_mysql.sql"));
        if (recreateDB) UtilityForTests.deleteDB(database.getAccessConfiguration());
        return database;
    }

    private String loadScript(String fileName) {
        try {
            return FileUtils.readFileToString(new File(baseFolder + fileName));
        } catch (IOException ex) {
            Assert.fail("Unable to load script file " + ex.getLocalizedMessage());
        }
        return null;
    }
}