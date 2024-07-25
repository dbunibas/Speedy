package speedy.utility.comparator;

import java.util.Comparator;
import speedy.exceptions.AlgebraException;
import speedy.model.database.AttributeRef;
import speedy.model.database.IDatabase;
import speedy.model.database.IValue;
import speedy.model.database.Tuple;

import static speedy.utility.SpeedyUtility.getCellValueForSorting;

public class TupleComparatorAttributeValue implements Comparator<Tuple>{

    private IDatabase db;
    private AttributeRef attribute;

    public TupleComparatorAttributeValue(IDatabase db, AttributeRef attribute) {
        this.db = db;
        this.attribute = attribute;
    }

    public int compare(Tuple t1, Tuple t2) {
        if (t1.getCell(attribute) == null || t2.getCell(attribute) == null) {
            throw new AlgebraException("Unable to find attribute " + attribute + " in tuples " + t1 + " - " + t2);
        }
//        IValue t1Value = t1.getCell(attribute).getValue();
//        IValue t2Value = t2.getCell(attribute).getValue();
//        return t2Value.toString().compareTo(t1Value.toString());
        String s1Value = getCellValueForSorting(db, t1.getCell(attribute));
        String s2Value = getCellValueForSorting(db, t2.getCell(attribute));
        return s2Value.compareTo(s1Value);
    }
}
