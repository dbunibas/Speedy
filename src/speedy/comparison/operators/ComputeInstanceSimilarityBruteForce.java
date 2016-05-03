package speedy.comparison.operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.SpeedyConstants;
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

    public enum ValueMatchResult {
        EQUAL_CONSTANTS, BOTH_NULLS, NULL_TO_CONSTANT, CONSTANT_TO_NULL, NOT_MATCHING
    }

    public InstanceMatch compare(IDatabase leftDb, IDatabase rightDb) {
        InstanceMatch instanceMatch = new InstanceMatch(leftDb, rightDb);
        List<TupleWithTable> sourceTuples = SpeedyUtility.extractAllTuplesFromDatabase(leftDb);
        List<TupleWithTable> destinationTuples = SpeedyUtility.extractAllTuplesFromDatabase(rightDb);
        TupleMatches tupleMatches = findTupleMatches(sourceTuples, destinationTuples);
        sortTupleMatches(tupleMatches);
        List<List<TupleMatch>> allTupleMatches = combineMatches(sourceTuples, tupleMatches);
        GenericListGeneratorIterator<TupleMatch> iterator = new GenericListGeneratorIterator<TupleMatch>(allTupleMatches);
        TupleMapping bestTupleMapping = null;
        double bestScore = 0;
        while (iterator.hasNext()) {
            TupleMapping nextTupleMapping = extractTupleMapping(iterator.next());
            if (nextTupleMapping == null) {
                continue;
            }
            double score = computeScore(nextTupleMapping);
            if (score > bestScore) {
                bestTupleMapping = nextTupleMapping;
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
        ValueMapping valueMapping = new ValueMapping();
        int score = 0;
        for (int i = 0; i < sourceTuple.getTuple().getCells().size(); i++) {
            if (sourceTuple.getTuple().getCells().get(i).getAttribute().equals(SpeedyConstants.OID)) {
                continue;
            }
            IValue sourceValue = sourceTuple.getTuple().getCells().get(i).getValue();
            IValue destinationValue = destinationTuple.getTuple().getCells().get(i).getValue();
            if (logger.isTraceEnabled()) logger.trace("Comparing values: " + sourceValue + ", " + destinationValue);
            ValueMatchResult matchResult = match(sourceValue, destinationValue);
            if (matchResult == ValueMatchResult.NOT_MATCHING) {
                if (logger.isTraceEnabled()) logger.trace("Values not match...");
                return null;
            }
            valueMapping = updateValueMapping(valueMapping, sourceValue, destinationValue, matchResult);
            if (valueMapping == null) {
                if (logger.isTraceEnabled()) logger.trace("Conflicting mapping for values...");
                return null;
            }
            score += score(matchResult);
        }
        TupleMatch tupleMatch = new TupleMatch(sourceTuple, destinationTuple, valueMapping, score);
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

    private int score(ValueMatchResult matchResult) {
        if (matchResult == ValueMatchResult.EQUAL_CONSTANTS) {
            return 1;
        }
        if (matchResult == ValueMatchResult.BOTH_NULLS) {
            return 1;
        }
        if (matchResult == ValueMatchResult.NULL_TO_CONSTANT) {
            return 0;
        }
        return 0;
    }

    private ValueMapping updateValueMapping(ValueMapping valueMapping, IValue sourceValue, IValue destinationValue, ValueMatchResult matchResult) {
        if (matchResult == ValueMatchResult.BOTH_NULLS || matchResult == ValueMatchResult.NULL_TO_CONSTANT) {
            IValue valueForSourceValue = valueMapping.getValueMapping(sourceValue);
            if (valueForSourceValue != null && !valueForSourceValue.equals(destinationValue)) {
                return null;
            }
            valueMapping.putValueMapping(sourceValue, destinationValue);
        }
        return valueMapping;
    }

    private void sortTupleMatches(TupleMatches tupleMatches) {
        for (TupleWithTable tuple : tupleMatches.getTuples()) {
            List<TupleMatch> matchesForTuple = tupleMatches.getMatchesForTuple(tuple);
            Collections.sort(matchesForTuple, new TupleMatchComparatorScore());
        }
    }

//    private List<List<TupleMatch>> combineMatches(List<TupleWithTable> sourceTuples, TupleMatches tupleMatches) {
//        GenericListGenerator<TupleMatch> generator = new GenericListGenerator<TupleMatch>();
//        List<List<TupleMatch>> allTupleMatches = new ArrayList<List<TupleMatch>>();
//        for (TupleWithTable sourceTuple : sourceTuples) {
//            allTupleMatches.add(tupleMatches.getMatchesForTuple(sourceTuple));
//        }
//        return generator.generateListsOfElements(allTupleMatches);
//    }
    
    private List<List<TupleMatch>> combineMatches(List<TupleWithTable> sourceTuples, TupleMatches tupleMatches) {
        List<List<TupleMatch>> allTupleMatches = new ArrayList<List<TupleMatch>>();
        for (TupleWithTable sourceTuple : sourceTuples) {
            allTupleMatches.add(tupleMatches.getMatchesForTuple(sourceTuple));
        }
        return allTupleMatches;
    }

    private TupleMapping extractTupleMapping(List<TupleMatch> tupleMatches) {
        TupleMapping homomorphism = new TupleMapping();
        for (TupleMatch tupleMatch : tupleMatches) {
            homomorphism = addTupleMatch(homomorphism, tupleMatch);
            if (homomorphism == null) {
                return null;
            }
            homomorphism.putTupleMapping(tupleMatch.getLeftTuple(), tupleMatch.getRightTuple());
        }
        return homomorphism;
    }

    private TupleMapping addTupleMatch(TupleMapping homomorphism, TupleMatch tupleMatch) {
        for (IValue sourceValue : tupleMatch.getValueMapping().getSourceValues()) {
            IValue destinationValue = tupleMatch.getValueMapping().getValueMapping(sourceValue);
            IValue valueForSourceValueInHomomorphism = homomorphism.getMappingForValue(sourceValue);
            if (valueForSourceValueInHomomorphism != null && !valueForSourceValueInHomomorphism.equals(destinationValue)) {
                return null;
            }
            homomorphism.addMappingForValue(sourceValue, destinationValue);
        }
        return homomorphism;
    }

    private double computeScore(TupleMapping nextTupleMapping) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
