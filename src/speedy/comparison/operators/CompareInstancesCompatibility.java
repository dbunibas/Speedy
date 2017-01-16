package speedy.comparison.operators;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    public InstanceMatchTask compare(IDatabase leftDb, IDatabase rightDb) {
        InstanceMatchTask instanceMatch = new InstanceMatchTask(leftDb, rightDb);
        List<TupleWithTable> sourceTuples = SpeedyUtility.extractAllTuplesFromDatabase(leftDb);
        List<TupleWithTable> destinationTuples = SpeedyUtility.extractAllTuplesFromDatabase(rightDb);
        List<TupleWithTable> firstDB = sourceTuples;
        List<TupleWithTable> secondDB = destinationTuples;
        boolean inverse = false;
        if (sourceTuples.size() > destinationTuples.size()) {
            firstDB = destinationTuples;
            secondDB = sourceTuples;
            inverse = true;
        }
        CompatibilityMap compatibilityMap = compatibleTupleFinder.find(firstDB, secondDB);
        if (logger.isDebugEnabled()) logger.debug("Compatibility map:\n" + compatibilityMap);
        TupleMatches tupleMatches = findTupleMatches(secondDB, compatibilityMap);
        ComparisonUtility.sortTupleMatches(tupleMatches);
        if (logger.isDebugEnabled()) logger.debug("TupleMatches: " + tupleMatches);
        TupleMapping bestTupleMapping = bestTupleMappingFinder.findBestTupleMapping(firstDB, tupleMatches);
        if (inverse) {
            bestTupleMapping = ComparisonUtility.invertMapping(bestTupleMapping); //TODO: Check score
        }
        instanceMatch.setTupleMapping(bestTupleMapping);
        return instanceMatch;
    }

    private TupleMatches findTupleMatches(List<TupleWithTable> secondDB, CompatibilityMap compatibilityMap) {
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
            //TODO: Add non matching tuples
//            List<TupleMatch> matchesForTuple = tupleMatches.getMatchesForTuple(secondTuple);
//            if (matchesForTuple == null) {
//                if (logger.isDebugEnabled()) logger.debug("Non matching tuple: " + secondTuple);
//                tupleMatches.addNonMatchingTuple(secondTuple);
//            }
        }
        return tupleMatches;
    }

}
