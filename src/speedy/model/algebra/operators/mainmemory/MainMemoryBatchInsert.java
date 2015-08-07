package speedy.model.algebra.operators.mainmemory;

import speedy.model.algebra.operators.IBatchInsert;
import speedy.model.algebra.operators.IInsertTuple;
import speedy.model.database.IDatabase;
import speedy.model.database.ITable;
import speedy.model.database.Tuple;

public class MainMemoryBatchInsert implements IBatchInsert {

    private IInsertTuple insertTuple = new MainMemoryInsertTuple();

    public void insert(ITable table, Tuple tuple, IDatabase database) {
        insertTuple.execute(table, tuple, null, database);
    }

    public void flush(IDatabase database) {
    }

}
