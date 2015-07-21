package speedy.model.algebra.operators;

import speedy.model.database.Tuple;
import java.util.Iterator;

public interface ITupleIterator extends Iterator<Tuple> {

    public void reset();

    public void close();

}
