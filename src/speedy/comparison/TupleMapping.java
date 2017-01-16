package speedy.comparison;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import speedy.model.database.IValue;
import speedy.utility.SpeedyUtility;

public class TupleMapping {

    private final Map<TupleWithTable, TupleWithTable> tupleMapping = new HashMap<TupleWithTable, TupleWithTable>();
    private ValueMapping leftToRightValueMapping = new ValueMapping();
    private ValueMapping rightToLeftValueMapping = new ValueMapping();
    private List<TupleWithTable> leftNonMatchingTuples = new ArrayList<TupleWithTable>();
    private List<TupleWithTable> rightNonMatchingTuples = new ArrayList<TupleWithTable>();
    private Double score;

    public void putTupleMapping(TupleWithTable sourceTuple, TupleWithTable destinationTuple) {
        this.tupleMapping.put(sourceTuple, destinationTuple);
    }

    public TupleWithTable getMappingForTuple(TupleWithTable tuple) {
        return this.tupleMapping.get(tuple);
    }

    public ValueMapping getLeftToRightValueMapping() {
        return leftToRightValueMapping;
    }

    public IValue getLeftToRightMappingForValue(IValue value) {
        return this.leftToRightValueMapping.getValueMapping(value);
    }

    public void addLeftToRightMappingForValue(IValue sourceValue, IValue destinationValue) {
        this.leftToRightValueMapping.putValueMapping(sourceValue, destinationValue);
    }

    public ValueMapping getRightToLeftValueMapping() {
        return rightToLeftValueMapping;
    }

    public IValue getRightToLeftMappingForValue(IValue value) {
        return this.rightToLeftValueMapping.getValueMapping(value);
    }

    public void addRightToLeftMappingForValue(IValue sourceValue, IValue destinationValue) {
        this.rightToLeftValueMapping.putValueMapping(sourceValue, destinationValue);
    }

    public List<TupleWithTable> getLeftNonMatchingTuples() {
        return leftNonMatchingTuples;
    }

    public void setLeftNonMatchingTuples(List<TupleWithTable> leftNonMatchingTuples) {
        this.leftNonMatchingTuples = leftNonMatchingTuples;
    }

    public List<TupleWithTable> getRightNonMatchingTuples() {
        return rightNonMatchingTuples;
    }

    public void setRightNonMatchingTuples(List<TupleWithTable> rightNonMatchingTuples) {
        this.rightNonMatchingTuples = rightNonMatchingTuples;
    }

    public void setLeftToRightValueMapping(ValueMapping leftToRightValueMapping) {
        this.leftToRightValueMapping = leftToRightValueMapping;
    }

    public void setRightToLeftValueMapping(ValueMapping rightToLeftValueMapping) {
        this.rightToLeftValueMapping = rightToLeftValueMapping;
    }

    public Map<TupleWithTable, TupleWithTable> getTupleMapping() {
        return tupleMapping;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public void addScore(double score) {
        this.score += score;
    }

    public boolean isEmpty() {
        return tupleMapping.isEmpty();
    }

    @Override
    public String toString() {
        if (this.isEmpty()) {
            return "no mapping";
        }
        return "----------------- Tuple Mapping ------------------"
                + SpeedyUtility.printMapCompact(tupleMapping)
                + (leftToRightValueMapping.isEmpty() ? "" : "\nValue mapping: " + leftToRightValueMapping)
                + (rightToLeftValueMapping.isEmpty() ? "" : "\nRight to left value mapping: " + rightToLeftValueMapping)
                + (score != null ? "\nScore: " + score + "\n" : "")
                + (!leftNonMatchingTuples.isEmpty() ? "Non matching left tuples=" + SpeedyUtility.printCollection(leftNonMatchingTuples) : "")
                + (!rightNonMatchingTuples.isEmpty() ? "Non matching right tuples=" + SpeedyUtility.printCollection(rightNonMatchingTuples) : "");
    }

}
