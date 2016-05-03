package speedy.comparison;

import java.util.HashMap;
import java.util.Map;
import speedy.model.database.IValue;
import speedy.utility.SpeedyUtility;

public class TupleMapping {
    
    private final Map<TupleWithTable, TupleWithTable> tupleMapping = new HashMap<TupleWithTable, TupleWithTable>();
    private final ValueMapping leftToRightValueMapping = new ValueMapping();
    private final ValueMapping rightToLeftValueMapping = new ValueMapping();
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
    
    public Map<TupleWithTable, TupleWithTable> getTupleMapping() {
        return tupleMapping;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "----------------- Tuple Mapping ------------------ [\n" 
                + SpeedyUtility.printMap(tupleMapping) 
                + "\n" + (leftToRightValueMapping.isEmpty() ? "" : "Left to right value mapping: " + leftToRightValueMapping) 
                + "\n" + (rightToLeftValueMapping.isEmpty() ? "" : "Right to left value mapping: " + rightToLeftValueMapping)
                + (score != null ? "\nScore: " + score : "") 
                + "\n]";
    }   

}
