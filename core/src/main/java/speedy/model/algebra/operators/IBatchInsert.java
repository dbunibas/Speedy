package speedy.model.algebra.operators;

import speedy.model.database.IDatabase;
import speedy.model.database.ITable;
import speedy.model.database.Tuple;

public interface IBatchInsert {

    void insert(ITable table, Tuple tuple, IDatabase database);
    
    void flush(IDatabase database);
}
