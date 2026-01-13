package speedy.model.algebra.udf;

import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.Tuple;

public interface IUserDefinedFunction {

    default ITupleIterator execute(ITupleIterator iterator) {
        throw new UnsupportedOperationException("iterator execution is not supported!");
    }

    default Object execute(Tuple tuple) {
        throw new UnsupportedOperationException("tuple execution is not supported!");
    }

}
