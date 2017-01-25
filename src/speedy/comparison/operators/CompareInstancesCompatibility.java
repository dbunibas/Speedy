package speedy.comparison.operators;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.ComparisonStats;
import speedy.comparison.ComparisonUtility;
import speedy.comparison.CompatibilityMap;
import speedy.comparison.InstanceMatchTask;
import speedy.comparison.TupleMapping;
import speedy.comparison.TupleMatch;
import speedy.comparison.TupleMatches;
import speedy.comparison.TupleWithTable;
import speedy.model.database.IDatabase;
import speedy.utility.SpeedyUtility;

public class CompareInstancesCompatibility implements IComputeInstanceSimilarity {

    private final static Logger logger = LoggerFactory.getLogger(CompareInstancesCompatibility.class);

    private final FindCompatibleTuples compatibleTupleFinder = new FindCompatibleTuples();
    private final CheckTupleMatch tupleMatcher = new CheckTupleMatch();
    private final FindBestTupleMapping bestTupleMappingFinder = new FindBestTupleMapping();
    private final FindNonMatchingTuples nonMatchingTuplesFinder = new FindNonMatchingTuples();

    public InstanceMatchTask compare(IDatabase leftDb, IDatabase rightDb) {
        long start = System.currentTimeMillis();
        InstanceMatchTask instanceMatch = new InstanceMatchTask(leftDb, rightDb);
        List<TupleWithTable> sourceTuples = SpeedyUtility.extractAllTuplesFromDatabase(leftDb);
        List<TupleWithTable> destinationTuples = SpeedyUtility.extractAllTuplesFromDatabase(rightDb);
        ComparisonStats.getInstance().addStat(ComparisonStats.PROCESS_INSTANCE_TIME, System.currentTimeMillis() - start);
        CompatibilityMap compatibilityMap = compatibleTupleFinder.find(sourceTuples, destinationTuples);
        if (logger.isDebugEnabled()) logger.debug("Compatibility map:\n" + compatibilityMap);
        TupleMatches tupleMatches = findTupleMatches(destinationTuples, compatibilityMap);
        ComparisonUtility.sortTupleMatches(tupleMatches);
        if (logger.isDebugEnabled()) logger.debug("TupleMatches: " + tupleMatches);
        TupleMapping bestTupleMapping = bestTupleMappingFinder.findBestTupleMapping(sourceTuples, destinationTuples, tupleMatches);
        nonMatchingTuplesFinder.find(sourceTuples, destinationTuples, bestTupleMapping);
        instanceMatch.setTupleMapping(bestTupleMapping);
        return instanceMatch;
    }

    private TupleMatches findTupleMatches(List<TupleWithTable> secondDB, CompatibilityMap compatibilityMap) {
        long start = System.currentTimeMillis();
        TupleMatches tupleMatches = new TupleMatches();
        for (TupleWithTable secondTuple : secondDB) {
            //We associate, for each tuple, a list of compatible destination tuples (i.e. they don't have different constants)
            for (TupleWithTable destinationTuple : compatibilityMap.getCompatibleTuples(secondTuple)) {
                TupleMatch match = tupleMatcher.checkMatch(destinationTuple, secondTuple);
                if (match != null) {
                    if (logger.isDebugEnabled()) logger.debug("Match found: " + match);
                    tupleMatches.addTupleMatch(destinationTuple, match);
                }
            }
        }
        ComparisonStats.getInstance().addStat(ComparisonStats.FIND_TUPLE_MATCHES, System.currentTimeMillis() - start);
        return tupleMatches;
    }

}
