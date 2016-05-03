package speedy.comparison.operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.SpeedyConstants;
import speedy.SpeedyConstants.ValueMatchResult;
import speedy.comparison.ComparisonConfiguration;
import speedy.comparison.TupleMapping;
import speedy.comparison.InstanceMatch;
import speedy.comparison.TupleMatch;
import speedy.comparison.TupleMatches;
import speedy.comparison.TupleWithTable;
import speedy.comparison.ValueMapping;
import speedy.model.database.ConstantValue;
import speedy.model.database.IDatabase;
import speedy.model.database.IValue;
import speedy.model.database.NullValue;
import speedy.utility.SpeedyUtility;
import speedy.utility.combinatorics.GenericListGeneratorIterator;
import speedy.utility.comparator.TupleMatchComparatorScore;

public class ComputeInstanceSimilarityBruteForce implements IComputeInstanceSimilarity {

    private final static Logger logger = LoggerFactory.getLogger(ComputeInstanceSimilarityBruteForce.class);

    public InstanceMatch compare(IDatabase leftDb, IDatabase rightDb) {
        InstanceMatch instanceMatch = new InstanceMatch(leftDb, rightDb);
        List<TupleWithTable> sourceTuples = SpeedyUtility.extractAllTuplesFromDatabase(leftDb);
        List<TupleWithTable> destinationTuples = SpeedyUtility.extractAllTuplesFromDatabase(rightDb);
        TupleMatches tupleMatches = findTupleMatches(sourceTuples, destinationTuples);
        sortTupleMatches(tupleMatches);
        if (logger.isTraceEnabled()) logger.trace(tupleMatches.toString());
        List<List<TupleMatch>> allTupleMatches = combineMatches(sourceTuples, tupleMatches);
        GenericListGeneratorIterator<TupleMatch> iterator = new GenericListGeneratorIterator<TupleMatch>(allTupleMatches);
        TupleMapping bestTupleMapping = null;
        double bestScore = 0;
        while (iterator.hasNext()) {
            List<TupleMatch> candidateTupleMatches = iterator.next();
            TupleMapping nextTupleMapping = extractTupleMapping(candidateTupleMatches);
            if (nextTupleMapping == null) {
                if (logger.isDebugEnabled()) logger.debug("Candidate match discarded...");
                continue;
            }
            if (logger.isDebugEnabled()) logger.debug("Analizing tuple mapping: " + nextTupleMapping);
            double similarityScore = computeSimilarityScore(candidateTupleMatches);
            nextTupleMapping.setScore(similarityScore);
            if (similarityScore > bestScore) {
                bestScore = similarityScore;
                bestTupleMapping = nextTupleMapping;
                if (logger.isDebugEnabled()) logger.debug("Found new best score: " + similarityScore);
            }
        }
        instanceMatch.setTupleMatch(bestTupleMapping);
        return instanceMatch;
    }

    private TupleMatches findTupleMatches(List<TupleWithTable> sourceTuples, List<TupleWithTable> destinationTuples) {
        TupleMatches tupleMatches = new TupleMatches();
        for (TupleWithTable sourceTuple : sourceTuples) {
            for (TupleWithTable destinationTuple : destinationTuples) {
                TupleMatch match = checkMatch(sourceTuple, destinationTuple);
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

    private TupleMatch checkMatch(TupleWithTable sourceTuple, TupleWithTable destinationTuple) {
        if (!sourceTuple.getTable().equals(destinationTuple.getTable())) {
            return null;
        }
        if (logger.isDebugEnabled()) logger.debug("Comparing tuple: " + sourceTuple + " to tuple " + destinationTuple);
        ValueMapping leftToRightValueMapping = new ValueMapping();
        ValueMapping rightToLeftValueMapping = new ValueMapping();
        int score = 0;
        for (int i = 0; i < sourceTuple.getTuple().getCells().size(); i++) {
            if (sourceTuple.getTuple().getCells().get(i).getAttribute().equals(SpeedyConstants.OID)) {
                continue;
            }
            IValue leftValue = sourceTuple.getTuple().getCells().get(i).getValue();
            IValue rightValue = destinationTuple.getTuple().getCells().get(i).getValue();
            if (logger.isTraceEnabled()) logger.trace("Comparing values: " + leftValue + ", " + rightValue);
            ValueMatchResult matchResult = match(leftValue, rightValue);
            if (matchResult == ValueMatchResult.NOT_MATCHING) {
                if (logger.isTraceEnabled()) logger.trace("Values not match...");
                return null;
            }
            boolean consistent = updateValueMappings(leftToRightValueMapping, rightToLeftValueMapping, leftValue, rightValue, matchResult);
            if (!consistent) {
                if (logger.isTraceEnabled()) logger.trace("Conflicting mapping for values...");
                return null;
            }
            score += score(matchResult);
        }
        if (!consistentValueMappings(leftToRightValueMapping, rightToLeftValueMapping)) {
            return null;
        }
        TupleMatch tupleMatch = new TupleMatch(sourceTuple, destinationTuple, leftToRightValueMapping, rightToLeftValueMapping, score);
        return tupleMatch;
    }

    private ValueMatchResult match(IValue sourceValue, IValue destinationValue) {
        if (sourceValue instanceof ConstantValue && destinationValue instanceof ConstantValue) {
            if (sourceValue.equals(destinationValue)) {
                return ValueMatchResult.EQUAL_CONSTANTS;
            }
        }
        if (sourceValue instanceof NullValue && destinationValue instanceof ConstantValue) {
            return ValueMatchResult.NULL_TO_CONSTANT;
        }
        if (sourceValue instanceof NullValue && destinationValue instanceof NullValue) {
            return ValueMatchResult.BOTH_NULLS;
        }
        if (ComparisonConfiguration.isTwoWayValueMapping() && sourceValue instanceof ConstantValue && destinationValue instanceof NullValue) {
            return ValueMatchResult.CONSTANT_TO_NULL;
        }
        return ValueMatchResult.NOT_MATCHING;
    }

    private double score(ValueMatchResult matchResult) {
        if (matchResult == ValueMatchResult.EQUAL_CONSTANTS) {
            return 1;
        }
        if (matchResult == ValueMatchResult.BOTH_NULLS) {
            return 1;
        }
        if (matchResult == ValueMatchResult.NULL_TO_CONSTANT) {
            return ComparisonConfiguration.getK();
        }
        if (matchResult == ValueMatchResult.CONSTANT_TO_NULL) {
            return ComparisonConfiguration.getK();
        }
        return 0;
    }

    private boolean updateValueMappings(ValueMapping leftToRightValueMapping, ValueMapping rightToLeftValueMapping, IValue leftValue, IValue rightValue, ValueMatchResult matchResult) {
        if (matchResult == ValueMatchResult.BOTH_NULLS || matchResult == ValueMatchResult.NULL_TO_CONSTANT) {
            IValue valueForSourceValue = leftToRightValueMapping.getValueMapping(leftValue);
            if (valueForSourceValue != null && !valueForSourceValue.equals(rightValue)) {
                return false;
            }
            leftToRightValueMapping.putValueMapping(leftValue, rightValue);
        }
        if (matchResult == ValueMatchResult.CONSTANT_TO_NULL) {
            IValue valueForDestinationValue = rightToLeftValueMapping.getValueMapping(rightValue);
            if (valueForDestinationValue != null && !valueForDestinationValue.equals(leftValue)) {
                return false;
            }
            rightToLeftValueMapping.putValueMapping(rightValue, leftValue);
        }
        return true;
    }

    private void sortTupleMatches(TupleMatches tupleMatches) {
        for (TupleWithTable tuple : tupleMatches.getTuples()) {
            List<TupleMatch> matchesForTuple = tupleMatches.getMatchesForTuple(tuple);
            Collections.sort(matchesForTuple, new TupleMatchComparatorScore());
        }
    }
    
    private List<List<TupleMatch>> combineMatches(List<TupleWithTable> sourceTuples, TupleMatches tupleMatches) {
        List<List<TupleMatch>> allTupleMatches = new ArrayList<List<TupleMatch>>();
        for (TupleWithTable sourceTuple : sourceTuples) {
            allTupleMatches.add(tupleMatches.getMatchesForTuple(sourceTuple));
        }
        return allTupleMatches;
    }

    private TupleMapping extractTupleMapping(List<TupleMatch> tupleMatches) {
        TupleMapping tupleMapping = new TupleMapping();
        for (TupleMatch tupleMatch : tupleMatches) {
            tupleMapping = addTupleMatch(tupleMapping, tupleMatch);
            if (tupleMapping == null) {
                return null;
            }
            tupleMapping.putTupleMapping(tupleMatch.getLeftTuple(), tupleMatch.getRightTuple());
        }
        if (!consistentValueMappings(tupleMapping.getLeftToRightValueMapping(), tupleMapping.getRightToLeftValueMapping())) {
            return null;
        }
        if (ComparisonConfiguration.isInjective() && !isInjective(tupleMapping)) {
            return null;
        }
        return tupleMapping;
    }

    private TupleMapping addTupleMatch(TupleMapping tupleMapping, TupleMatch tupleMatch) {
        for (IValue leftValue : tupleMatch.getLeftToRightValueMapping().getKeys()) {
            IValue rightValue = tupleMatch.getLeftToRightValueMapping().getValueMapping(leftValue);
            IValue valueForLeftValueInMapping = tupleMapping.getLeftToRightMappingForValue(leftValue);
            if (valueForLeftValueInMapping != null && !valueForLeftValueInMapping.equals(rightValue)) {
                return null;
            }
            tupleMapping.addLeftToRightMappingForValue(leftValue, rightValue);
        }
        for (IValue rightValue : tupleMatch.getRightToLeftValueMapping().getKeys()) {
            IValue leftValue = tupleMatch.getRightToLeftValueMapping().getValueMapping(rightValue);
            IValue valueForRightValueInMapping = tupleMapping.getRightToLeftMappingForValue(rightValue);
            if (valueForRightValueInMapping != null && !valueForRightValueInMapping.equals(leftValue)) {
                return null;
            }
            tupleMapping.addRightToLeftMappingForValue(rightValue, leftValue);
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
    
    private boolean consistentValueMappings(ValueMapping leftToRightValueMapping, ValueMapping rightToLeftValueMapping) {
        for (IValue leftValue : leftToRightValueMapping.getKeys()) {
            IValue rightValue = leftToRightValueMapping.getValueMapping(leftValue);
            IValue leftValueInRightToLeft = rightToLeftValueMapping.getValueMapping(rightValue);
            if (leftValueInRightToLeft != null) {
                return false;
            }
        }
        return true;
    }
    
}
