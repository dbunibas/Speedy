package speedy.model.algebra.udf;

import speedy.model.algebra.operators.ITupleIterator;

public interface IUserDefinedFunction {

    // TODO: add databases?
    ITupleIterator execute(ITupleIterator iterator);

}
