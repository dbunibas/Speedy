package speedy.comparison;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import speedy.SpeedyConstants;
import speedy.model.database.AttributeRef;
import speedy.model.database.Cell;
import speedy.model.database.ConstantValue;
import speedy.model.database.IValue;
import speedy.model.database.Tuple;

public class ComparisonUtility {

    public static Set<AttributeRef> findAttributesWithGroundValue(Tuple tuple) {
        Set<AttributeRef> attributes = new HashSet<AttributeRef>();
        for (Cell cell : tuple.getCells()) {
            if (cell.getAttribute().equals(SpeedyConstants.OID)) {
                continue;
            }
            if (!(cell.getValue() instanceof ConstantValue)) {
                continue;
            }
            attributes.add(cell.getAttributeRef());
        }
        return attributes;
    }

    public static boolean valueMappingsAreCompatible(ValueMapping leftToRightValueMapping, ValueMapping rightToLeftValueMapping) {
        for (IValue leftValue : leftToRightValueMapping.getKeys()) {
            IValue rightValue = leftToRightValueMapping.getValueMapping(leftValue);
            IValue leftValueInRightToLeft = rightToLeftValueMapping.getValueMapping(rightValue);
            if (leftValueInRightToLeft != null) {
                return false;
            }
        }
        return true;
    }

    public static boolean isTupleMatchCompatibleWithTupleMapping(TupleMapping tupleMapping, TupleMatch tupleMatch) {
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

    public static void updateTupleMapping(TupleMapping tupleMapping, TupleMatch tupleMatch) {
        for (IValue leftValue : tupleMatch.getLeftToRightValueMapping().getKeys()) {
            IValue rightValue = tupleMatch.getLeftToRightValueMapping().getValueMapping(leftValue);
            tupleMapping.addLeftToRightMappingForValue(leftValue, rightValue);
        }
        for (IValue rightValue : tupleMatch.getRightToLeftValueMapping().getKeys()) {
            IValue leftValue = tupleMatch.getRightToLeftValueMapping().getValueMapping(rightValue);
            tupleMapping.addRightToLeftMappingForValue(rightValue, leftValue);
        }
    }

}
