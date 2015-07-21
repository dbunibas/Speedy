package speedy.model.database.operators;

import speedy.model.algebra.IAlgebraOperator;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.IDatabase;
import speedy.model.database.ResultInfo;

public interface IRunQuery {

    ITupleIterator run(IAlgebraOperator query, IDatabase source, IDatabase target);

    ResultInfo getSize(IAlgebraOperator query, IDatabase source, IDatabase target);

}
