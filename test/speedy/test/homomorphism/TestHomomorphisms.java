package speedy.test.homomorphism;

import java.io.File;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.InstanceMatch;
import speedy.comparison.operators.FindHomomorphism;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.AttributeRef;
import speedy.model.database.Cell;
import speedy.model.database.IDatabase;
import speedy.model.database.ITable;
import speedy.model.database.IValue;
import speedy.model.database.NullValue;
import speedy.model.database.Tuple;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.database.dbms.InitDBConfiguration;
import speedy.persistence.DAODBMSDatabase;
import speedy.persistence.DAOMainMemoryDatabase;
import speedy.persistence.file.CSVFile;
import speedy.utility.test.UtilityForTests;

public class TestHomomorphisms extends TestCase {

    private final static Logger logger = LoggerFactory.getLogger(TestHomomorphisms.class);

    private FindHomomorphism homomorphismFinder = new FindHomomorphism();
    private static String BASE_FOLDER = "/resources/homomorphism/";
    private DAOMainMemoryDatabase dao = new DAOMainMemoryDatabase();

    public void test1() {
        IDatabase sourceDb = loadDatabase("01", "source");
        IDatabase destinationDb = loadDatabase("01", "destination");
        InstanceMatch result = homomorphismFinder.findHomomorphism(sourceDb, destinationDb);
        logger.info(result.toString());
        assert (result.getNonMatchingTuples() == null);
    }

    public void test2() {
        IDatabase sourceDb = loadDatabase("02", "source");
        IDatabase destinationDb = loadDatabase("02", "destination");
        InstanceMatch result = homomorphismFinder.findHomomorphism(sourceDb, destinationDb);
        logger.info(result.toString());
        assert (result.getNonMatchingTuples() == null);
    }

    public void test3() {
        String baseAbsFolder = UtilityForTests.getAbsoluteFileName(BASE_FOLDER);
        IDatabase sourceDb = dao.loadCSVDatabase(baseAbsFolder + "s2/", ',', null);
        IDatabase destinationDb = dao.loadCSVDatabase(baseAbsFolder + "d2/", ',', null);
        InstanceMatch result = homomorphismFinder.findHomomorphism(sourceDb, destinationDb);
        logger.info(result.toString());
        assert (result.getNonMatchingTuples() == null);
        ITable table = sourceDb.getTable("table1");
        ITupleIterator tupleIterator = table.getTupleIterator();
        Tuple firstTuple = tupleIterator.next();
        AttributeRef attributeRefA = new AttributeRef("table1", "A");
        Cell cell = firstTuple.getCell(attributeRefA);
        assertNotNull(cell);
        IValue value = cell.getValue();
        assertTrue(value instanceof NullValue);
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
