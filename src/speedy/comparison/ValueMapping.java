package speedy.comparison;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import speedy.model.database.IValue;
import speedy.utility.SpeedyUtility;

public class ValueMapping {
    
    private Map<IValue, IValue> map = new HashMap<IValue, IValue>();
    
    public void putValueMapping(IValue value, IValue correspondingValue) {
        this.map.put(value, correspondingValue);
    }

    public IValue getValueMapping(IValue value) {
        return this.map.get(value);
    }

    public Set<IValue> getSourceValues() {
        return this.map.keySet();
    }
    
    public String toString() {
        return "----------- Value Mapping -------------\n" + SpeedyUtility.printMap(map);
    }
}
