package speedy.model.algebra.operators;

import speedy.model.database.IDatabase;
import speedy.model.database.ITable;
import speedy.model.database.Tuple;

public interface IInsertTuple {

    void execute(ITable table, Tuple tuple, IDatabase source, IDatabase target);

}
