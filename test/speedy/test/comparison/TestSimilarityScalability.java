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

public class TestSimilarityScalability extends TestCase {

    private final static Logger logger = LoggerFactory.getLogger(TestSimilarityScalability.class);

    private IComputeInstanceSimilarity similarityCheckerBruteForce = new ComputeInstanceSimilarityBruteForce();
    private IComputeInstanceSimilarity similarityCheckerCompatibility = new CompareInstancesCompatibility();
    private IComputeInstanceSimilarity similarityCheckerHashing = new CompareInstancesHashing();

    public void testDoctors() {
        String baseFolder = "/Users/donatello/Dropbox (Informatica)/Shared Folders/Enzo/Omomorfismi/";
        ComparisonConfiguration.setStringSkolemPrefixes(new String[]{"_SK", "_:e"});
        ComparisonConfiguration.setConvertSkolemInHash(true);
        String[] sizes = new String[]{"10k", "100k", "500k", "1m"};
        for (String size : sizes) {
            if (logger.isInfoEnabled()) logger.info("Size: " + size);
            IDatabase leftDb = ComparisonUtilityTest.loadDatabase(baseFolder + "Llunatic-" + size);
            IDatabase rightDb = ComparisonUtilityTest.loadDatabase(baseFolder + "RDFox-" + size);
            execute(leftDb, rightDb, similarityCheckerBruteForce);
            execute(leftDb, rightDb, similarityCheckerCompatibility);
            execute(leftDb, rightDb, similarityCheckerHashing);
        }
    }

    private void execute(IDatabase leftDb, IDatabase rightDb, IComputeInstanceSimilarity similarityChecker) {
        ComparisonStats.getInstance().resetStatistics();
        InstanceMatchTask result = similarityChecker.compare(leftDb, rightDb);
        if (logger.isTraceEnabled()) logger.trace(result.toString());
        if (logger.isInfoEnabled()) logger.info("----------- " + similarityChecker.getClass().getName() + " -----------------");
        if (logger.isInfoEnabled()) logger.info("Score: " + result.getTupleMapping().getScore());
        if (logger.isInfoEnabled()) logger.info("Non matching tuples: " + result.getTupleMapping().getLeftNonMatchingTuples().size());
        if (logger.isInfoEnabled()) logger.info(ComparisonStats.getInstance().toString());
        if (logger.isInfoEnabled()) logger.info("--------------------------------------------------");
    }
}
