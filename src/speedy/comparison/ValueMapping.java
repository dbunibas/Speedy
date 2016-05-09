package speedy.comparison;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import speedy.model.database.IValue;

public class ValueMapping implements Cloneable {

    private Map<IValue, IValue> map = new HashMap<IValue, IValue>();

    public void putValueMapping(IValue value, IValue correspondingValue) {
        this.map.put(value, correspondingValue);
    }

    public IValue getValueMapping(IValue value) {
        return this.map.get(value);
    }

    public Set<IValue> getKeys() {
        return this.map.keySet();
    }

    public Collection<IValue> getValues() {
        return this.map.values();
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public String toString() {
        return map.toString();
    }

//    @Override
//    public ValueMapping clone() { //SLOW
//        try {
//            ValueMapping clone = (ValueMapping) super.clone();
//            clone.map = new HashMap<IValue, IValue>(map);
//            return clone;
//        } catch (CloneNotSupportedException ex) {
//            return null;
//        }
//    }
}
