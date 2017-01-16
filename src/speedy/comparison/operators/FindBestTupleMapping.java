package speedy.comparison.operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.ComparisonConfiguration;
import speedy.comparison.ComparisonUtility;
import speedy.comparison.TupleMapping;
import speedy.comparison.TupleMatch;
import speedy.comparison.TupleMatches;
import speedy.comparison.TupleWithTable;
import speedy.utility.SpeedyUtility;
import speedy.utility.combinatorics.GenericListGeneratorIterator;
import speedy.utility.combinatorics.GenericPowersetGenerator;

public class FindBestTupleMapping {

    private final static Logger logger = LoggerFactory.getLogger(FindBestTupleMapping.class);
    private final GenericPowersetGenerator<TupleMatch> powersetGenerator = new GenericPowersetGenerator<TupleMatch>();
    private final CheckTupleMatchCompatibility compatibilityChecker = new CheckTupleMatchCompatibility();

    public TupleMapping findBestTupleMapping(List<TupleWithTable> sourceTuples, TupleMatches tupleMatches) {
        if (logger.isDebugEnabled()) logger.debug("Finding tuple mapping for\n" + SpeedyUtility.printCollection(sourceTuples) + "\n with " + tupleMatches);
        List<List<TupleMatch>> allTupleMatches = createListOfLists(sourceTuples, tupleMatches);
        if (logger.isDebugEnabled()) logger.debug("All tuple matches:\n" + SpeedyUtility.printCollection(allTupleMatches));
        GenericListGeneratorIterator<TupleMatch> iterator = new GenericListGeneratorIterator<TupleMatch>(allTupleMatches);
        TupleMapping bestTupleMapping = null;
        double bestScore = 0;
        while (iterator.hasNext()) {
            List<List<TupleMatch>> powerSet = powersetGenerator.generatePowerSet(iterator.next());
            for (List<TupleMatch> candidateTupleMatches : powerSet) {
                TupleMapping nextTupleMapping = extractTupleMapping(candidateTupleMatches);
                if (logger.isDebugEnabled()) logger.debug("Candidate tuple mapping: " + nextTupleMapping);
                if (nextTupleMapping == null) {
                    if (logger.isDebugEnabled()) logger.debug("Candidate match discarded...");
                    continue;
                }
                nextTupleMapping.getLeftNonMatchingTuples().addAll(tupleMatches.getNonMatchingTuples());
                double similarityScore = computeSimilarityScore(candidateTupleMatches);
                nextTupleMapping.setScore(similarityScore);
                if (similarityScore > bestScore) {
                    bestScore = similarityScore;
                    bestTupleMapping = nextTupleMapping;
                    if (logger.isDebugEnabled()) logger.debug("Found new best score: " + similarityScore);
                }
            }
        }
        return bestTupleMapping;
    }

    public TupleMapping extractTupleMapping(List<TupleMatch> tupleMatches) {
        if (logger.isDebugEnabled()) logger.debug("Extracting mapping using tuple matches:\n" + SpeedyUtility.printCollection(tupleMatches));
        TupleMapping tupleMapping = new TupleMapping();
        for (TupleMatch tupleMatch : tupleMatches) {
            if (!compatibilityChecker.isTupleMatchCompatibleWithTupleMapping(tupleMapping, tupleMatch)) {
//                return null;
                tupleMapping.getLeftNonMatchingTuples().add(tupleMatch.getLeftTuple());
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

    private boolean isInjective(TupleMapping tupleMapping) {
        Collection<TupleWithTable> imageTuples = tupleMapping.getTupleMapping().values();
        Set<TupleWithTable> imageTupleSet = new HashSet<TupleWithTable>(imageTuples);
        return imageTuples.size() == imageTupleSet.size();
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

    private double computeSimilarityScore(List<TupleMatch> tupleMatches) {
        double similarityScore = 0;
        for (TupleMatch tupleMatch : tupleMatches) {
            similarityScore += tupleMatch.getSimilarity();
        }
        return similarityScore;
    }
}
