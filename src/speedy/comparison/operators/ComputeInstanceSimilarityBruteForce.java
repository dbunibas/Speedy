package speedy.comparison.operators;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.ComparisonUtility;
import speedy.comparison.TupleMapping;
import speedy.comparison.InstanceMatchTask;
import speedy.comparison.TupleMatch;
import speedy.comparison.TupleMatches;
import speedy.comparison.TupleWithTable;
import speedy.model.database.IDatabase;
import speedy.utility.SpeedyUtility;

public class ComputeInstanceSimilarityBruteForce implements IComputeInstanceSimilarity {

    private final static Logger logger = LoggerFactory.getLogger(ComputeInstanceSimilarityBruteForce.class);
    private final CheckTupleMatch tupleMatcher = new CheckTupleMatch();
    private final FindBestTupleMapping bestTupleMappingFinder = new FindBestTupleMapping();

    public InstanceMatchTask compare(IDatabase leftDb, IDatabase rightDb) {
        InstanceMatchTask instanceMatch = new InstanceMatchTask(leftDb, rightDb);
        List<TupleWithTable> sourceTuples = SpeedyUtility.extractAllTuplesFromDatabase(leftDb);
        List<TupleWithTable> destinationTuples = SpeedyUtility.extractAllTuplesFromDatabase(rightDb);
        TupleMatches tupleMatches = findTupleMatches(sourceTuples, destinationTuples);
        ComparisonUtility.sortTupleMatches(tupleMatches);
        if (logger.isTraceEnabled()) logger.trace(tupleMatches.toString());
        TupleMapping bestTupleMapping = bestTupleMappingFinder.findBestTupleMapping(sourceTuples, tupleMatches);
        instanceMatch.setTupleMapping(bestTupleMapping);
        return instanceMatch;
    }

    private TupleMatches findTupleMatches(List<TupleWithTable> sourceTuples, List<TupleWithTable> destinationTuples) {
        TupleMatches tupleMatches = new TupleMatches();
        for (TupleWithTable sourceTuple : sourceTuples) {
            //We associate, for each source tuple, a list of compatible destination tuples (i.e. they don't have different constants)
            for (TupleWithTable destinationTuple : destinationTuples) {
                TupleMatch match = tupleMatcher.checkMatch(sourceTuple, destinationTuple);
                if (match != null) {
                    if (logger.isDebugEnabled()) logger.debug("Match found: " + match);
                    tupleMatches.addTupleMatch(sourceTuple, match);
                }
            }
            List<TupleMatch> matchesForTuple = tupleMatches.getMatchesForTuple(sourceTuple);
            if (matchesForTuple == null) {
                if (logger.isDebugEnabled()) logger.debug("Non matching tuple: " + sourceTuple);
                tupleMatches.addNonMatchingTuple(sourceTuple);
            }
        }
        return tupleMatches;
    }

}
