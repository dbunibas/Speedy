package speedy.utility.comparator;

import java.util.Comparator;
import speedy.comparison.TupleMatch;

public class TupleMatchComparatorScore implements Comparator<TupleMatch> {

    public int compare(TupleMatch o1, TupleMatch o2) {
        return o2.getSimilarity().compareTo(o1.getSimilarity());
    }

}
