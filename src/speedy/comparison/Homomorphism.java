package speedy.comparison;

import java.util.HashMap;
import java.util.Map;
import speedy.model.database.IValue;
import speedy.utility.SpeedyUtility;

public class Homomorphism {
    
    private final ValueMapping valueMapping = new ValueMapping();
    private final Map<TupleWithTable, TupleWithTable> tupleMapping = new HashMap<TupleWithTable, TupleWithTable>();

    public void putTupleMapping(TupleWithTable sourceTuple, TupleWithTable destinationTuple) {
        this.tupleMapping.put(sourceTuple, destinationTuple);
    }
    
    public TupleWithTable getMappingForTuple(TupleWithTable tuple) {
        return this.tupleMapping.get(tuple);
    }
    
    public ValueMapping getValueMapping() {
        return valueMapping;
    }
    
    public IValue getMappingForValue(IValue value) {
        return this.valueMapping.getValueMapping(value);
    }

    public void addMappingForValue(IValue sourceValue, IValue destinationValue) {
        this.valueMapping.putValueMapping(sourceValue, destinationValue);
    }
    
    public Map<TupleWithTable, TupleWithTable> getTupleMapping() {
        return tupleMapping;
    }

    @Override
    public String toString() {
        return "Homomorphism [" 
                + "\n----------------- Tuple Mapping ------------------\n" + SpeedyUtility.printMap(tupleMapping) 
                + "\n" + valueMapping 
                + "\n]";
    }   

}
