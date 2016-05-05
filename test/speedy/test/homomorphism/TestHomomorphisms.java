package speedy.test.homomorphism;

import java.io.File;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.InstanceMatch;
import speedy.comparison.operators.FindHomomorphism;
import speedy.model.database.IDatabase;
import speedy.model.database.mainmemory.MainMemoryDB;
import speedy.persistence.DAOMainMemoryDatabase;
import speedy.utility.test.UtilityForTests;

public class TestHomomorphisms extends TestCase {

    private final static Logger logger = LoggerFactory.getLogger(TestHomomorphisms.class);

    private FindHomomorphism homomorphismFinder = new FindHomomorphism();
    private static String BASE_FOLDER = "/resources/homomorphism/";
    private DAOMainMemoryDatabase dao = new DAOMainMemoryDatabase();

//    public void test1() {
//        IDatabase leftDb = loadDatabase("01/left");
//        IDatabase rightDb = loadDatabase("01/right");
//        InstanceMatch result = homomorphismFinder.findHomomorphism(leftDb, rightDb);
//        logger.info(result.toString());
//        assert (result.getNonMatchingTuples() == null);
//    }
//
//    public void test2() {
//        IDatabase leftDb = loadDatabase("02/left");
//        IDatabase rightDb = loadDatabase("02/right");
//        InstanceMatch result = homomorphismFinder.findHomomorphism(leftDb, rightDb);
//        logger.info(result.toString());
//        assert (result.getNonMatchingTuples() == null);
//    }
//
//    public void test3() {
//        IDatabase leftDb = loadDatabase("03/left");
//        IDatabase rightDb = loadDatabase("03/right");
//        InstanceMatch result = homomorphismFinder.findHomomorphism(leftDb, rightDb);
//        logger.info(result.toString());
//        assert (result.getNonMatchingTuples() == null);
//    }

    public void test4Isomorphism() {
        IDatabase leftDb = loadDatabase("04/left");
        IDatabase rightDb = loadDatabase("04/right");
        InstanceMatch result = homomorphismFinder.findHomomorphism(leftDb, rightDb);
        logger.info(result.toString());
        assert (result.getNonMatchingTuples() == null);
        assert (result.isIsomorphism());
    }

    private IDatabase loadDatabase(String folderName) {
        String folder = UtilityForTests.getAbsoluteFileName(BASE_FOLDER + File.separator + folderName);
        MainMemoryDB database = dao.loadCSVDatabase(folder, ',', null);
        logger.info(database.printInstances(true));
        return database;
    }

}
