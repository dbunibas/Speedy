package speedy.test.comparison;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.ComparisonConfiguration;
import speedy.comparison.ComparisonStats;
import speedy.comparison.InstanceMatchTask;
import speedy.comparison.operators.CompareInstancesCompatibility;
import speedy.comparison.operators.CompareInstancesHashing;
import speedy.comparison.operators.ComputeInstanceSimilarityBruteForce;
import speedy.comparison.operators.IComputeInstanceSimilarity;
import speedy.model.database.IDatabase;
import speedy.test.ComparisonUtilityTest;
import speedy.utility.PrintUtility;

public class TSimilarityScalability extends TestCase {

    private final static Logger logger = LoggerFactory.getLogger(TSimilarityScalability.class);

    private IComputeInstanceSimilarity similarityCheckerBruteForce = new ComputeInstanceSimilarityBruteForce();
    private IComputeInstanceSimilarity similarityCheckerCompatibility = new CompareInstancesCompatibility();
    private IComputeInstanceSimilarity similarityCheckerHashing = new CompareInstancesHashing();

    public void testDoctors() {
        String baseFolder = "/Temp/comparison/doctors/";
        ComparisonConfiguration.setStringSkolemPrefixes(new String[]{"_SK", "_:e"});
        ComparisonConfiguration.setConvertSkolemInHash(true);
        ComparisonConfiguration.setInjective(true);
        ComparisonConfiguration.setFunctional(true);
        String[] sizes = new String[]{"10k", "100k", "500k", "1m"};
//        String[] sizes = new String[]{"1m"};
        for (String size : sizes) {
            PrintUtility.printMessage("================ Size: " + size + " ================");
            IDatabase leftDb = ComparisonUtilityTest.loadDatabase(baseFolder + "Llunatic-" + size);
            IDatabase rightDb = ComparisonUtilityTest.loadDatabase(baseFolder + "RDFox-" + size);
//            execute(leftDb, rightDb, similarityCheckerHashing);
            execute(leftDb, rightDb, similarityCheckerCompatibility);
//            execute(leftDb, rightDb, similarityCheckerBruteForce);
            PrintUtility.printMessage("===============================================");
        }
    }

    private void execute(IDatabase leftDb, IDatabase rightDb, IComputeInstanceSimilarity similarityChecker) {
        ComparisonStats.getInstance().resetStatistics();
        PrintUtility.printMessage("----------- " + similarityChecker.getClass().getSimpleName() + " -----------------");
        long start = System.currentTimeMillis();
        InstanceMatchTask result = similarityChecker.compare(leftDb, rightDb);
        long totalTime = System.currentTimeMillis() - start;
        if (logger.isTraceEnabled()) logger.trace(result.toString());
        PrintUtility.printInformation("Total Time: " + totalTime + " ms");
        PrintUtility.printMessage("Score: " + result.getTupleMapping().getScore());
        PrintUtility.printMessage("Non matching tuples: " + result.getTupleMapping().getLeftNonMatchingTuples().size());
        PrintUtility.printMessage(ComparisonStats.getInstance().toString());
        PrintUtility.printMessage("--------------------------------------------------");
    }
}
