package speedy.comparison.operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.ComparisonConfiguration;
import speedy.comparison.ComparisonUtility;
import speedy.comparison.TupleMapping;
import speedy.comparison.InstanceMatchTask;
import speedy.comparison.TupleMatch;
import speedy.comparison.TupleMatches;
import speedy.comparison.TupleWithTable;
import speedy.model.database.IDatabase;
import speedy.utility.SpeedyUtility;
import speedy.utility.combinatorics.GenericListGeneratorIterator;
import speedy.utility.combinatorics.GenericPowersetGenerator;
import speedy.utility.comparator.TupleMatchComparatorScore;

public class ComputeInstanceSimilarityBruteForce implements IComputeInstanceSimilarity {

    private final static Logger logger = LoggerFactory.getLogger(ComputeInstanceSimilarityBruteForce.class);
    private final CheckTupleMatch tupleMatcher = new CheckTupleMatch();
    private final GenericPowersetGenerator<TupleMatch> powersetGenerator = new GenericPowersetGenerator<TupleMatch>();

    public InstanceMatchTask compare(IDatabase leftDb, IDatabase rightDb) {
        InstanceMatchTask instanceMatch = new InstanceMatchTask(leftDb, rightDb);
        List<TupleWithTable> sourceTuples = SpeedyUtility.extractAllTuplesFromDatabase(leftDb);
        List<TupleWithTable> destinationTuples = SpeedyUtility.extractAllTuplesFromDatabase(rightDb);
        TupleMatches tupleMatches = findTupleMatches(sourceTuples, destinationTuples);
        sortTupleMatches(tupleMatches);
        if (logger.isTraceEnabled()) logger.trace(tupleMatches.toString());
        List<List<TupleMatch>> allTupleMatches = createListOfLists(sourceTuples, tupleMatches);
        GenericListGeneratorIterator<TupleMatch> iterator = new GenericListGeneratorIterator<TupleMatch>(allTupleMatches);
        TupleMapping bestTupleMapping = null;
        double bestScore = 0;
        while (iterator.hasNext()) {
            List<List<TupleMatch>> powerSet = powersetGenerator.generatePowerSet(iterator.next());
            for (List<TupleMatch> candidateTupleMatches : powerSet) {
                TupleMapping nextTupleMapping = extractTupleMapping(candidateTupleMatches);
                if (nextTupleMapping == null) {
                    if (logger.isDebugEnabled()) logger.debug("Candidate match discarded...");
                    continue;
                }
                nextTupleMapping.getNonMatchingTuples().addAll(tupleMatches.getNonMatchingTuples());
                if (logger.isDebugEnabled()) logger.debug("Analizing tuple mapping: " + nextTupleMapping);
                double similarityScore = computeSimilarityScore(candidateTupleMatches);
                nextTupleMapping.setScore(similarityScore);
                if (similarityScore > bestScore) {
                    bestScore = similarityScore;
                    bestTupleMapping = nextTupleMapping;
                    if (logger.isDebugEnabled()) logger.debug("Found new best score: " + similarityScore);
                }
            }
        }
        instanceMatch.setTupleMapping(bestTupleMapping);
        return instanceMatch;
    }

    private TupleMatches findTupleMatches(List<TupleWithTable> sourceTuples, List<TupleWithTable> destinationTuples) {
        TupleMatches tupleMatches = new TupleMatches();
        for (TupleWithTable sourceTuple : sourceTuples) {
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

    private void sortTupleMatches(TupleMatches tupleMatches) {
        for (TupleWithTable tuple : tupleMatches.getTuples()) {
            List<TupleMatch> matchesForTuple = tupleMatches.getMatchesForTuple(tuple);
            Collections.sort(matchesForTuple, new TupleMatchComparatorScore());
        }
    }

    private List<List<TupleMatch>> createListOfLists(List<TupleWithTable> sourceTuples, TupleMatches tupleMatches) {
        List<List<TupleMatch>> allTupleMatches = new ArrayList<List<TupleMatch>>();
        for (TupleWithTable sourceTuple : sourceTuples) {
            List<TupleMatch> tupleMatchesForTuple = tupleMatches.getMatchesForTuple(sourceTuple);
            if (tupleMatchesForTuple == null || tupleMatchesForTuple.isEmpty()) {
                continue;
            }
            allTupleMatches.add(tupleMatchesForTuple);
        }
        return allTupleMatches;
    }

    private TupleMapping extractTupleMapping(List<TupleMatch> tupleMatches) {
        TupleMapping tupleMapping = new TupleMapping();
        for (TupleMatch tupleMatch : tupleMatches) {
            if (!ComparisonUtility.isTupleMatchCompatibleWithTupleMapping(tupleMapping, tupleMatch)) {
//                return null;
                tupleMapping.getNonMatchingTuples().add(tupleMatch.getLeftTuple());
                continue;
            }
            ComparisonUtility.updateTupleMapping(tupleMapping, tupleMatch);
            tupleMapping.putTupleMapping(tupleMatch.getLeftTuple(), tupleMatch.getRightTuple());
        }
        if (!ComparisonUtility.valueMappingsAreCompatible(tupleMapping.getLeftToRightValueMapping(), tupleMapping.getRightToLeftValueMapping())) {
            return null;
        }
        if (ComparisonConfiguration.isInjective() && !isInjective(tupleMapping)) {
            return null;
        }
        return tupleMapping;
    }

    private double computeSimilarityScore(List<TupleMatch> tupleMatches) {
        double similarityScore = 0;
        for (TupleMatch tupleMatch : tupleMatches) {
            similarityScore += tupleMatch.getSimilarity();
        }
        return similarityScore;
    }

    private boolean isInjective(TupleMapping tupleMapping) {
        Collection<TupleWithTable> imageTuples = tupleMapping.getTupleMapping().values();
        Set<TupleWithTable> imageTupleSet = new HashSet<TupleWithTable>(imageTuples);
        return imageTuples.size() == imageTupleSet.size();
    }

}
