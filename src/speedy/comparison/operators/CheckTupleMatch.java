package speedy.comparison.operators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.SpeedyConstants;
import speedy.comparison.ComparisonConfiguration;
import speedy.comparison.ComparisonStats;
import speedy.comparison.ComparisonUtility;
import speedy.comparison.TupleMatch;
import speedy.comparison.TupleWithTable;
import speedy.comparison.ValueMapping;
import speedy.model.database.ConstantValue;
import speedy.model.database.IValue;
import speedy.model.database.NullValue;

public class CheckTupleMatch {

    private final static Logger logger = LoggerFactory.getLogger(CheckTupleMatch.class);

    public TupleMatch checkMatch(TupleWithTable leftTuple, TupleWithTable rightTuple) {
        long start = System.currentTimeMillis();
        TupleMatch result = check(leftTuple, rightTuple);
        long end = System.currentTimeMillis();
        ComparisonStats.getInstance().addStat(ComparisonStats.CHECK_TUPLE_MATCH_TIME, end - start);
        return result;
    }

    private TupleMatch check(TupleWithTable leftTuple, TupleWithTable rightTuple) {
        if (logger.isDebugEnabled()) logger.debug("Comparing tuple: " + leftTuple + " to tuple " + rightTuple);
        ValueMapping leftToRightValueMapping = new ValueMapping();
        ValueMapping rightToLeftValueMapping = new ValueMapping();
        if (!leftTuple.getTable().equals(rightTuple.getTable())) {
            return null;
        }
        double score = 0.0;
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
            boolean consistent = updateValueMappings(leftToRightValueMapping, rightToLeftValueMapping, leftValue, rightValue, matchResult);
            if (!consistent) {
                if (logger.isTraceEnabled()) logger.trace("Conflicting mapping for values...");
                return null;
            }
            double matchScore = score(matchResult);
            if (logger.isTraceEnabled()) logger.trace("Match score " + matchScore);
            score += matchScore;
        }
        if (!ComparisonUtility.valueMappingsAreCompatible(leftToRightValueMapping, rightToLeftValueMapping)) {
            return null;
        }
        if (logger.isTraceEnabled()) logger.trace("Total score: " + score);
        TupleMatch tupleMatch = new TupleMatch(leftTuple, rightTuple, leftToRightValueMapping, rightToLeftValueMapping, score);
        return tupleMatch;
    }

    private SpeedyConstants.ValueMatchResult match(IValue sourceValue, IValue destinationValue) {
        if (sourceValue instanceof ConstantValue && destinationValue instanceof ConstantValue) {
            if (sourceValue.equals(destinationValue)) {
                return SpeedyConstants.ValueMatchResult.EQUAL_CONSTANTS;
            }
        }
        if (sourceValue instanceof NullValue && destinationValue instanceof ConstantValue) {
            return SpeedyConstants.ValueMatchResult.NULL_TO_CONSTANT;
        }
        if (sourceValue instanceof NullValue && destinationValue instanceof NullValue) {
            return SpeedyConstants.ValueMatchResult.BOTH_NULLS;
        }
        if (ComparisonConfiguration.isTwoWayValueMapping() && sourceValue instanceof ConstantValue && destinationValue instanceof NullValue) {
            return SpeedyConstants.ValueMatchResult.CONSTANT_TO_NULL;
        }
        return SpeedyConstants.ValueMatchResult.NOT_MATCHING;
    }

    private double score(SpeedyConstants.ValueMatchResult matchResult) {
        if (matchResult == SpeedyConstants.ValueMatchResult.EQUAL_CONSTANTS) {
            return 1;
        }
        if (matchResult == SpeedyConstants.ValueMatchResult.BOTH_NULLS) {
            return 1;
        }
        if (matchResult == SpeedyConstants.ValueMatchResult.NULL_TO_CONSTANT) {
            return ComparisonConfiguration.getK();
        }
        if (matchResult == SpeedyConstants.ValueMatchResult.CONSTANT_TO_NULL) {
            return ComparisonConfiguration.getK();
        }
        return 0;
    }

    private boolean updateValueMappings(ValueMapping leftToRightValueMapping, ValueMapping rightToLeftValueMapping, IValue leftValue, IValue rightValue, SpeedyConstants.ValueMatchResult matchResult) {
        if (matchResult == SpeedyConstants.ValueMatchResult.BOTH_NULLS || matchResult == SpeedyConstants.ValueMatchResult.NULL_TO_CONSTANT) {
            IValue valueForSourceValue = leftToRightValueMapping.getValueMapping(leftValue);
            if (valueForSourceValue != null && !valueForSourceValue.equals(rightValue)) {
                return false;
            }
            leftToRightValueMapping.putValueMapping(leftValue, rightValue);
        }
        if (matchResult == SpeedyConstants.ValueMatchResult.CONSTANT_TO_NULL) {
            IValue valueForDestinationValue = rightToLeftValueMapping.getValueMapping(rightValue);
            if (valueForDestinationValue != null && !valueForDestinationValue.equals(leftValue)) {
                return false;
            }
            rightToLeftValueMapping.putValueMapping(rightValue, leftValue);
        }
        return true;
    }
}
