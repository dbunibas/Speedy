package speedy.test.comparison;

import java.io.File;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.InstanceMatch;
import speedy.comparison.operators.ComputeInstanceSimilarityBruteForce;
import speedy.comparison.operators.IComputeInstanceSimilarity;
import speedy.model.database.IDatabase;
import speedy.model.database.mainmemory.MainMemoryDB;
import speedy.persistence.DAOMainMemoryDatabase;
import speedy.utility.test.UtilityForTests;

public class TestSimilarityBruteForce extends TestCase {
    
    private final static Logger logger = LoggerFactory.getLogger(TestSimilarityBruteForce.class);

    private IComputeInstanceSimilarity similarityChecker = new ComputeInstanceSimilarityBruteForce();
    private DAOMainMemoryDatabase dao = new DAOMainMemoryDatabase();
    private static String BASE_FOLDER = "/resources/similarity/";

    public void test1() {
        IDatabase leftDb = loadDatabase("left1");
        IDatabase rightDb = loadDatabase("right1");
        InstanceMatch result = similarityChecker.compare(leftDb, rightDb);
        logger.info(result.toString());
//        assert(result.getNonMatchingTuples() == null);
    }

    public void test2() {
        IDatabase leftDb = loadDatabase("left2");
        IDatabase rightDb = loadDatabase("right2");
        InstanceMatch result = similarityChecker.compare(leftDb, rightDb);
        logger.info(result.toString());
//        assert(result.getNonMatchingTuples() == null);
    }
 
    private IDatabase loadDatabase(String folderName) {
        String folder = UtilityForTests.getAbsoluteFileName(BASE_FOLDER + File.separator + folderName);
        MainMemoryDB database = dao.loadCSVDatabase(folder, ',', null);
        logger.info(database.printInstances(true));
        return database;
    }

}
