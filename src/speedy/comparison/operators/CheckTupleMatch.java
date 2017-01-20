package speedy.comparison.operators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.SpeedyConstants;
import speedy.comparison.ComparisonConfiguration;
import speedy.comparison.ComparisonStats;
import speedy.comparison.ComparisonUtility;
import speedy.comparison.TupleMatch;
import speedy.comparison.TupleWithTable;
import speedy.comparison.ValueMappings;
import speedy.model.database.ConstantValue;
import speedy.model.database.IValue;
import speedy.utility.SpeedyUtility;

public class CheckTupleMatch {

    private final static Logger logger = LoggerFactory.getLogger(CheckTupleMatch.class);
    private final CheckTupleMatchCompatibility compatibilityChecker = new CheckTupleMatchCompatibility();

    public TupleMatch checkMatch(TupleWithTable leftTuple, TupleWithTable rightTuple) {
        long start = System.currentTimeMillis();
        TupleMatch result = check(leftTuple, rightTuple);
        long end = System.currentTimeMillis();
        ComparisonStats.getInstance().addStat(ComparisonStats.CHECK_TUPLE_MATCH_TIME, end - start);
        return result;
    }

    private TupleMatch check(TupleWithTable leftTuple, TupleWithTable rightTuple) {
        if (logger.isDebugEnabled()) logger.debug("Comparing tuple: " + leftTuple + " to tuple " + rightTuple);
        if (!leftTuple.getTable().equals(rightTuple.getTable())) {
            return null;
        }
        ValueMappings valueMappings = new ValueMappings();
        double scoreEstimate = 0.0;
        for (int i = 0; i < leftTuple.getTuple().getCells().size(); i++) {
            if (leftTuple.getTuple().getCells().get(i).getAttribute().equals(SpeedyConstants.OID)) {
                continue;
            }
            IValue leftValue = leftTuple.getTuple().getCells().get(i).getValue();
            IValue rightValue = rightTuple.getTuple().getCells().get(i).getValue();
            if (logger.isTraceEnabled()) logger.trace("Comparing values: '" + leftValue + "', '" + rightValue + "'");
            SpeedyConstants.ValueMatchResult matchResult = match(leftValue, rightValue);
            if (matchResult == SpeedyConstants.ValueMatchResult.NOT_MATCHING) {
                if (logger.isTraceEnabled()) logger.trace("Values not match...");
                return null;
            }
            boolean consistent = updateValueMappings(valueMappings, leftValue, rightValue, matchResult);
            if (!consistent) {
                if (logger.isTraceEnabled()) logger.trace("Conflicting mapping for values...");
                return null;
            }
            double matchScore = scoreEstimate(matchResult);
            if (logger.isTraceEnabled()) logger.trace("Match score " + matchScore);
            scoreEstimate += matchScore;
        }
        TupleMatch tupleMatch = new TupleMatch(leftTuple, rightTuple, valueMappings, scoreEstimate);
        boolean compatible = compatibilityChecker.checkCompatibilityAndMerge(ComparisonUtility.getEmptyValueMappings(), tupleMatch);
        if (!compatible) {
            if (logger.isDebugEnabled()) logger.debug("Inconsistent value mappings, discarding...");
            return null;
        }
        tupleMatch.setValueMappings(valueMappings);
        if (logger.isDebugEnabled()) logger.debug("** Corrected tuple match: " + tupleMatch);
        return tupleMatch;
    }

    public SpeedyConstants.ValueMatchResult match(IValue sourceValue, IValue destinationValue) {
        if (sourceValue instanceof ConstantValue && destinationValue instanceof ConstantValue) {
            if (sourceValue.equals(destinationValue)) {
                return SpeedyConstants.ValueMatchResult.EQUAL_CONSTANTS;
            }
        }
        if (SpeedyUtility.isPlaceholder(sourceValue) && destinationValue instanceof ConstantValue) {
            return SpeedyConstants.ValueMatchResult.PLACEHOLDER_TO_CONSTANT;
        }
        if (SpeedyUtility.isPlaceholder(sourceValue) && SpeedyUtility.isPlaceholder(destinationValue)) {
            return SpeedyConstants.ValueMatchResult.BOTH_PLACEHOLDER;
        }
        if (ComparisonConfiguration.isTwoWayValueMapping() && sourceValue instanceof ConstantValue && SpeedyUtility.isPlaceholder(destinationValue)) {
            return SpeedyConstants.ValueMatchResult.CONSTANT_TO_PLACEHOLDER;
        }
        return SpeedyConstants.ValueMatchResult.NOT_MATCHING;
    }

    public double scoreEstimate(SpeedyConstants.ValueMatchResult matchResult) {
        if (matchResult == SpeedyConstants.ValueMatchResult.EQUAL_CONSTANTS) {
            return 1;
        }
        if (matchResult == SpeedyConstants.ValueMatchResult.BOTH_PLACEHOLDER) {
            return 1;
        }
        if (matchResult == SpeedyConstants.ValueMatchResult.PLACEHOLDER_TO_CONSTANT) {
            return ComparisonConfiguration.getK();
        }
        if (matchResult == SpeedyConstants.ValueMatchResult.CONSTANT_TO_PLACEHOLDER) {
            return ComparisonConfiguration.getK();
        }
        return 0;
    }

    private boolean updateValueMappings(ValueMappings valueMappings, IValue leftValue, IValue rightValue, SpeedyConstants.ValueMatchResult matchResult) {
        if (matchResult == SpeedyConstants.ValueMatchResult.BOTH_PLACEHOLDER || matchResult == SpeedyConstants.ValueMatchResult.PLACEHOLDER_TO_CONSTANT) {
            IValue valueForSourceValue = valueMappings.getLeftToRightValueMapping().getValueMapping(leftValue);
            if (valueForSourceValue != null && !valueForSourceValue.equals(rightValue)) {
                return false;
            }
            valueMappings.getLeftToRightValueMapping().putValueMapping(leftValue, rightValue);
        }
        if (matchResult == SpeedyConstants.ValueMatchResult.CONSTANT_TO_PLACEHOLDER) {
            IValue valueForDestinationValue = valueMappings.getRightToLeftValueMapping().getValueMapping(rightValue);
            if (valueForDestinationValue != null && !valueForDestinationValue.equals(leftValue)) {
                return false;
            }
            valueMappings.getRightToLeftValueMapping().putValueMapping(rightValue, leftValue);
        }
        return true;
    }
}
