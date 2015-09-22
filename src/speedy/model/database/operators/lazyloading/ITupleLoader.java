package speedy.model.database.operators.lazyloading;

import speedy.model.database.Tuple;
import speedy.model.database.TupleOID;

public interface ITupleLoader {

    Tuple loadTuple();

    public TupleOID getOid();
}
