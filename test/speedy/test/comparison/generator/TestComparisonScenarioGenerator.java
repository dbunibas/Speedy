package speedy.test.comparison.generator;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.ComparisonConfiguration;
import speedy.comparison.ComparisonStats;
import speedy.comparison.InstanceMatchTask;
import speedy.comparison.generator.ComparisonScenarioGenerator;
import speedy.comparison.generator.InstancePair;
import speedy.comparison.operators.CompareInstancesCompatibility;
import speedy.comparison.operators.CompareInstancesHashing;
import speedy.comparison.operators.ComputeInstanceSimilarityBruteForce;
import speedy.comparison.operators.IComputeInstanceSimilarity;
import speedy.model.database.IDatabase;
import speedy.persistence.file.operators.ExportCSVFile;
import speedy.test.ComparisonUtilityTest;
import speedy.utility.PrintUtility;
import speedy.utility.SpeedyUtility;

public class TestComparisonScenarioGenerator extends TestCase {

    private final static Logger logger = LoggerFactory.getLogger(TestComparisonScenarioGenerator.class);
    private final ComparisonScenarioGenerator generator = new ComparisonScenarioGenerator();
    private final IComputeInstanceSimilarity similarityCheckerBruteForce = new ComputeInstanceSimilarityBruteForce();
    private final IComputeInstanceSimilarity similarityCheckerCompatibility = new CompareInstancesCompatibility();
    private final IComputeInstanceSimilarity similarityCheckerHashing = new CompareInstancesHashing();

    public void test() {
//        setInjectiveFunctionalMapping();   //no new tuples
        setNonInjectiveFunctionalMapping();//new left tuples (default)
//        setInjectiveNonFunctionalMapping();//new right tuples
//        setNonInjectiveNonFunctionalMapping();//new left/right tuples
        ComparisonConfiguration.setTwoWayValueMapping(true); //Change constants in nulls in both instances (default)
        ComparisonConfiguration.setForceExaustiveSearch(false);
        String sourceFile = "/Temp/comparison/redundancy/conference/";
        IDatabase sourceDB = ComparisonUtilityTest.loadDatabase(sourceFile);
        InstancePair instancePair = generator.generate(sourceDB);
        if (logger.isTraceEnabled()) logger.trace(instancePair.toString());
        ExportCSVFile exporter = new ExportCSVFile();
        exporter.exportDatabase(instancePair.getLeftDB(), true, false, "/Temp/comparison/redundancy/conference_left/");
        exporter.exportDatabase(instancePair.getRightDB(), true, false, "/Temp/comparison/redundancy/conference_right/");
        execute(instancePair.getLeftDB(), instancePair.getRightDB(), similarityCheckerHashing);
        execute(instancePair.getLeftDB(), instancePair.getRightDB(), similarityCheckerCompatibility);
        execute(instancePair.getLeftDB(), instancePair.getRightDB(), similarityCheckerBruteForce);
    }

    private void execute(IDatabase leftDb, IDatabase rightDb, IComputeInstanceSimilarity similarityChecker) {
        ComparisonStats.getInstance().resetStatistics();
        PrintUtility.printInformation("----------- " + similarityChecker.getClass().getSimpleName() + " -----------------");
        long start = System.currentTimeMillis();
        InstanceMatchTask result = similarityChecker.compare(leftDb, rightDb);
        long totalTime = System.currentTimeMillis() - start;
        if (logger.isTraceEnabled()) logger.trace(result.toString());
        PrintUtility.printInformation("Total Time: " + totalTime + " ms");
        PrintUtility.printInformation("Score: " + result.getTupleMapping().getScore());
        PrintUtility.printMessage("Non matching left tuples: " + result.getTupleMapping().getLeftNonMatchingTuples().size());
        if (!result.getTupleMapping().getLeftNonMatchingTuples().isEmpty() && result.getTupleMapping().getLeftNonMatchingTuples().size() < 5) {
            PrintUtility.printMessage(SpeedyUtility.printCollection(result.getTupleMapping().getLeftNonMatchingTuples(), "\t"));
        }
        PrintUtility.printMessage("Non matching right tuples: " + result.getTupleMapping().getRightNonMatchingTuples().size());
        if (!result.getTupleMapping().getRightNonMatchingTuples().isEmpty() && result.getTupleMapping().getRightNonMatchingTuples().size() < 5) {
            PrintUtility.printMessage(SpeedyUtility.printCollection(result.getTupleMapping().getRightNonMatchingTuples(), "\t"));
        }
        PrintUtility.printMessage(ComparisonStats.getInstance().toString());
        PrintUtility.printMessage("--------------------------------------------------");
    }

    private void setInjectiveFunctionalMapping() {
        ComparisonConfiguration.setInjective(true);
        ComparisonConfiguration.setFunctional(true);
    }

    private void setNonInjectiveFunctionalMapping() {
        ComparisonConfiguration.setInjective(false);
        ComparisonConfiguration.setFunctional(true);
    }

    private void setInjectiveNonFunctionalMapping() {
        ComparisonConfiguration.setInjective(true);
        ComparisonConfiguration.setFunctional(false);
    }

    private void setNonInjectiveNonFunctionalMapping() {
        ComparisonConfiguration.setInjective(false);
        ComparisonConfiguration.setFunctional(false);
    }

}
