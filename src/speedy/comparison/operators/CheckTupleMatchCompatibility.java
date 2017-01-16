package speedy.comparison.operators;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.ComparisonStats;
import speedy.comparison.TupleMapping;
import speedy.comparison.TupleMatch;
import speedy.comparison.ValueMapping;
import speedy.model.database.IValue;

public class CheckTupleMatchCompatibility {

    private final static Logger logger = LoggerFactory.getLogger(CheckTupleMatchCompatibility.class);

    public boolean isTupleMatchCompatibleWithTupleMapping(TupleMapping tupleMapping, TupleMatch tupleMatch) {
        if (logger.isDebugEnabled()) logger.debug("Checking compatibility btw " + tupleMatch + "\n\t with tuple mapping: " + tupleMapping);
        long start = System.currentTimeMillis();
        ValueMapping leftToRightValueMapping = tupleMapping.getLeftToRightValueMapping();
        ValueMapping rightToLeftValueMapping = tupleMapping.getRightToLeftValueMapping();
//        ValueMapping leftToRightValueMapping = tupleMapping.getLeftToRightValueMapping().clone();
//        ValueMapping rightToLeftValueMapping = tupleMapping.getRightToLeftValueMapping().clone();
        Map<IValue, IValue> currentLeftToRightValueMapping = new HashMap<IValue, IValue>();
        Map<IValue, IValue> currentRightToLeftValueMapping = new HashMap<IValue, IValue>();
        for (IValue leftValue : tupleMatch.getLeftToRightValueMapping().getKeys()) {
            IValue rightValue = tupleMatch.getLeftToRightValueMapping().getValueMapping(leftValue);
            IValue valueForLeftValueInMapping = currentLeftToRightValueMapping.get(leftValue);
            if (valueForLeftValueInMapping == null) {
                valueForLeftValueInMapping = leftToRightValueMapping.getValueMapping(leftValue);
            }
            if (valueForLeftValueInMapping != null && !valueForLeftValueInMapping.equals(rightValue)) {
                ComparisonStats.getInstance().addStat(ComparisonStats.CHECK_TUPLE_MATCH_COMPATIBILITY_TIME, System.currentTimeMillis() - start);
                return false;
            }
//            leftToRightValueMapping.putValueMapping(leftValue, rightValue);
            currentLeftToRightValueMapping.put(leftValue, rightValue);
        }
        for (IValue rightValue : tupleMatch.getRightToLeftValueMapping().getKeys()) {
            IValue leftValue = tupleMatch.getRightToLeftValueMapping().getValueMapping(rightValue);
            IValue valueForRightValueInMapping = currentRightToLeftValueMapping.get(rightValue);
            if (valueForRightValueInMapping == null) {
                valueForRightValueInMapping = rightToLeftValueMapping.getValueMapping(rightValue);
            }
            if (valueForRightValueInMapping != null && !valueForRightValueInMapping.equals(leftValue)) {
                ComparisonStats.getInstance().addStat(ComparisonStats.CHECK_TUPLE_MATCH_COMPATIBILITY_TIME, System.currentTimeMillis() - start);
                return false;
            }
//            rightToLeftValueMapping.putValueMapping(rightValue, leftValue);
            currentRightToLeftValueMapping.put(leftValue, rightValue);
        }
        ComparisonStats.getInstance().addStat(ComparisonStats.CHECK_TUPLE_MATCH_COMPATIBILITY_TIME, System.currentTimeMillis() - start);
        return true;
    }
}
