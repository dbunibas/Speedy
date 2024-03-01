package speedy.utility.comparator;

import java.util.Comparator;
import speedy.exceptions.AlgebraException;
import speedy.model.database.AttributeRef;
import speedy.model.database.IValue;
import speedy.model.database.Tuple;

public class TupleComparatorAttributeValue implements Comparator<Tuple>{

    private AttributeRef attribute;

    public TupleComparatorAttributeValue(AttributeRef attribute) {
        this.attribute = attribute;
    }

    public int compare(Tuple t1, Tuple t2) {
        if (t1.getCell(attribute) == null || t2.getCell(attribute) == null) {
            throw new AlgebraException("Unable to find attribute " + attribute + " in tuples " + t1 + " - " + t2);
        }
        IValue t1Value = t1.getCell(attribute).getValue();
        IValue t2Value = t2.getCell(attribute).getValue();
        return t2Value.toString().compareTo(t1Value.toString());
    }
}
