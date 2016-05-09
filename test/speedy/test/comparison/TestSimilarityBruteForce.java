package speedy.test.comparison;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.InstanceMatchTask;
import speedy.comparison.TupleWithTable;
import speedy.comparison.operators.ComputeInstanceSimilarityBruteForce;
import speedy.comparison.operators.IComputeInstanceSimilarity;
import speedy.model.database.AttributeRef;
import speedy.model.database.IDatabase;
import speedy.test.ComparisonUtilityTest;

public class TestSimilarityBruteForce extends TestCase {

    private final static Logger logger = LoggerFactory.getLogger(TestSimilarityBruteForce.class);

    private IComputeInstanceSimilarity similarityChecker = new ComputeInstanceSimilarityBruteForce();
    private static String BASE_FOLDER = "/resources/similarity/";

//    public void test1() {
//        IDatabase leftDb = UtilityTest.loadDatabase("01/left", BASE_FOLDER);
//        IDatabase rightDb = UtilityTest.loadDatabase("01/right", BASE_FOLDER);
//        InstanceMatchTask result = similarityChecker.compare(leftDb, rightDb);
//        logger.info(result.toString());
//        assertEquals(0, result.getTupleMapping().getNonMatchingTuples().size());
//        assertEquals(15.0, result.getTupleMapping().getScore());
//    }

    public void test2() {
        IDatabase leftDb = ComparisonUtilityTest.loadDatabase("02/left", BASE_FOLDER);
        IDatabase rightDb = ComparisonUtilityTest.loadDatabase("02/right", BASE_FOLDER);
        InstanceMatchTask result = similarityChecker.compare(leftDb, rightDb);
        logger.info(result.toString());
        assertEquals(1, result.getTupleMapping().getNonMatchingTuples().size());
    }
//
//    public void test3() {
//        IDatabase leftDb = UtilityTest.loadDatabase("03/left", BASE_FOLDER);
//        IDatabase rightDb = UtilityTest.loadDatabase("03/right", BASE_FOLDER);
//        InstanceMatchTask result = similarityChecker.compare(leftDb, rightDb);
//        logger.info(result.toString());
//        assertEquals(3, result.getTupleMapping().getNonMatchingTuples().size());
//        for (TupleWithTable nonMatchingTuple : result.getTupleMapping().getNonMatchingTuples()) {
//            if (nonMatchingTuple.getTable().equals("s")) {
//                assertEquals("_N11", nonMatchingTuple.getTuple().getCell(new AttributeRef("s", "A")).getValue().toString());
//            }
//        }
//    }

}
