package speedy.model.algebra.operators;

import speedy.model.algebra.IAlgebraOperator;
import speedy.model.database.IDatabase;

public interface IDelete {

    boolean execute(String tableName, IAlgebraOperator sourceQuery, IDatabase source, IDatabase target);

}
