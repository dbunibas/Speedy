package speedy.model.database.operators;

import speedy.model.algebra.IAlgebraOperator;
import speedy.model.database.IDatabase;

public interface IExplainQuery {

    public long explain(IAlgebraOperator query, IDatabase source, IDatabase target) ;

}
