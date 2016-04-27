package speedy.test.homomorphism;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.HomomorphismCheckResult;
import speedy.comparison.operators.FindHomomorphism;
import speedy.model.database.IDatabase;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.database.dbms.InitDBConfiguration;
import speedy.persistence.DAODBMSDatabase;
import speedy.persistence.file.CSVFile;
import speedy.utility.test.UtilityForTests;

public class TestHomomorphisms extends TestCase {
    
    private final static Logger logger = LoggerFactory.getLogger(TestHomomorphisms.class);

    private FindHomomorphism homomorphismFinder = new FindHomomorphism();
    private static String BASE_FOLDER = "/resources/homomorphism/";

    public void test1() {
        IDatabase sourceDb = loadDatabase("01", "source");
        IDatabase destinationDb = loadDatabase("01", "destination");
        HomomorphismCheckResult result = homomorphismFinder.findHomomorphism(sourceDb, destinationDb);
        logger.info(result.toString());
        assert(result.getNonMatchingTuples() == null);
    }
    
    public void test2() {
        IDatabase sourceDb = loadDatabase("02", "source");
        IDatabase destinationDb = loadDatabase("02", "destination");
        HomomorphismCheckResult result = homomorphismFinder.findHomomorphism(sourceDb, destinationDb);
        logger.info(result.toString());
        assert(result.getNonMatchingTuples() == null);
    }

    private IDatabase loadDatabase(String expName, String schemaName) {
        DAODBMSDatabase daoDatabase = new DAODBMSDatabase();
        String driver = "org.postgresql.Driver";
        String uri = "jdbc:postgresql:speedy_comparison_" + expName;
        String login = "pguser";
        String password = "pguser";
        DBMSDB database = daoDatabase.loadDatabase(driver, uri, schemaName, login, password);
        UtilityForTests.deleteDB(database.getAccessConfiguration());
        InitDBConfiguration initDBConfiguration = database.getInitDBConfiguration();
        initDBConfiguration.setCreateTablesFromFiles(true);
        String baseAbsFolder = UtilityForTests.getAbsoluteFileName(BASE_FOLDER);
        initDBConfiguration.addFileToImportForTable("r", new CSVFile(baseAbsFolder + expName + "-" + schemaName + ".csv", ',', '"'));
//        database.initDBMS();
        return database;
    }

}
