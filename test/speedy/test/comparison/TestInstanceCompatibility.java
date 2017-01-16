package speedy.test.comparison;

import junit.framework.TestCase;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.InstanceMatchTask;
import speedy.comparison.TupleWithTable;
import speedy.comparison.operators.CompareInstancesCompatibility;
import speedy.comparison.operators.IComputeInstanceSimilarity;
import speedy.model.database.AttributeRef;
import speedy.model.database.IDatabase;
import speedy.model.database.NullValue;
import speedy.test.ComparisonUtilityTest;

public class TestInstanceCompatibility extends TestCase {

    private final static Logger logger = LoggerFactory.getLogger(TestInstanceCompatibility.class);

    private IComputeInstanceSimilarity similarityChecker = new CompareInstancesCompatibility();
    private static String BASE_FOLDER = "/resources/similarity/";

//    public void test1() {
//        IDatabase leftDb = ComparisonUtilityTest.loadDatabase("01/left", BASE_FOLDER);
//        IDatabase rightDb = ComparisonUtilityTest.loadDatabase("01/right", BASE_FOLDER);
//        InstanceMatchTask result = similarityChecker.compare(leftDb, rightDb);
//        logger.info(result.toString());
//        assertEquals(0, result.getTupleMapping().getLeftNonMatchingTuples().size());
//        assertEquals(15.5, result.getTupleMapping().getScore());
//    }
//
//    public void test2() {
//        IDatabase leftDb = ComparisonUtilityTest.loadDatabase("02/left", BASE_FOLDER);
//        IDatabase rightDb = ComparisonUtilityTest.loadDatabase("02/right", BASE_FOLDER);
//        InstanceMatchTask result = similarityChecker.compare(leftDb, rightDb);
//        logger.info(result.toString());
//        assertEquals(1, result.getTupleMapping().getLeftNonMatchingTuples().size());
//        assertEquals(10.0, result.getTupleMapping().getScore());
//    }
//
//    public void test3() {
//        IDatabase leftDb = ComparisonUtilityTest.loadDatabase("03/left", BASE_FOLDER);
//        IDatabase rightDb = ComparisonUtilityTest.loadDatabase("03/right", BASE_FOLDER);
//        InstanceMatchTask result = similarityChecker.compare(leftDb, rightDb);
//        logger.info(result.toString());
//        assertEquals(3, result.getTupleMapping().getLeftNonMatchingTuples().size());
//        assertEquals(14.5, result.getTupleMapping().getScore());
//        for (TupleWithTable nonMatchingTuple : result.getTupleMapping().getLeftNonMatchingTuples()) {
//            if (nonMatchingTuple.getTable().equals("s")) {
//                assertEquals("_N11", nonMatchingTuple.getTuple().getCell(new AttributeRef("s", "A")).getValue().toString());
//            }
//        }
//    }
//
//    public void test4() {
//        IDatabase leftDb = ComparisonUtilityTest.loadDatabase("04/left", BASE_FOLDER);
//        IDatabase rightDb = ComparisonUtilityTest.loadDatabase("04/right", BASE_FOLDER);
//        InstanceMatchTask result = similarityChecker.compare(leftDb, rightDb);
//        logger.info(result.toString());
//        assertNotNull(result.getTupleMapping());
//        assertEquals(0, result.getTupleMapping().getLeftNonMatchingTuples().size());
//        assertEquals(3.0, result.getTupleMapping().getScore());
//    }
//
//    public void test5() {
//        IDatabase leftDb = ComparisonUtilityTest.loadDatabase("05/left", BASE_FOLDER);
//        IDatabase rightDb = ComparisonUtilityTest.loadDatabase("05/right", BASE_FOLDER);
//        InstanceMatchTask result = similarityChecker.compare(leftDb, rightDb);
//        logger.info(result.toString());
//        assertNotNull(result.getTupleMapping());
//        assertEquals(0, result.getTupleMapping().getLeftNonMatchingTuples().size());
//        assertEquals(6.0, result.getTupleMapping().getScore());
//    }
//    public void test6() {
//        IDatabase leftDb = ComparisonUtilityTest.loadDatabase("06/left", BASE_FOLDER);
//        IDatabase rightDb = ComparisonUtilityTest.loadDatabase("06/right", BASE_FOLDER);
//        InstanceMatchTask result = similarityChecker.compare(leftDb, rightDb);
//        logger.info(result.toString());
//        assertNotNull(result.getTupleMapping());
//        assertEquals("3", result.getTupleMapping().getLeftToRightMappingForValue(new NullValue("_N1")).toString());
//        assertEquals("_N3", result.getTupleMapping().getLeftToRightMappingForValue(new NullValue("_N4")).toString());
//        assertEquals("2", result.getTupleMapping().getRightToLeftMappingForValue(new NullValue("_N3")).toString());
//        assertEquals("3", result.getTupleMapping().getLeftToRightMappingForValue(new NullValue("_N5")).toString());
//        assertEquals(1, result.getTupleMapping().getLeftNonMatchingTuples().size());
////        assertEquals(6.0, result.getTupleMapping().getScore());
//    }
    public void test7() {
        IDatabase leftDb = ComparisonUtilityTest.loadDatabase("07/left", BASE_FOLDER);
        IDatabase rightDb = ComparisonUtilityTest.loadDatabase("07/right", BASE_FOLDER);
        InstanceMatchTask result = similarityChecker.compare(leftDb, rightDb);
        logger.info(result.toString());
        assertNotNull(result.getTupleMapping());
        assertEquals(1, result.getTupleMapping().getTupleMapping().size());
        assertEquals("1", result.getTupleMapping().getLeftToRightMappingForValue(new NullValue("_N2")).toString());
        assertEquals("2", result.getTupleMapping().getLeftToRightMappingForValue(new NullValue("_N3")).toString());
        assertEquals("3", result.getTupleMapping().getRightToLeftMappingForValue(new NullValue("_N1")).toString());
//        assertEquals(1, result.getTupleMapping().getLeftNonMatchingTuples().size());
//        assertEquals(6.0, result.getTupleMapping().getScore());
    }

}
