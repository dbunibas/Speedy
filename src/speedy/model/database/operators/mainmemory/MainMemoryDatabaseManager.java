package speedy.model.database.operators.mainmemory;

import speedy.model.database.IDatabase;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.database.operators.IDatabaseManager;

public class MainMemoryDatabaseManager implements IDatabaseManager {

    public IDatabase cloneTarget(DBMSDB target, String suffix) {
        return target.clone();
    }

    public void restoreTarget(IDatabase original, DBMSDB target, String suffix) {
    }

    public void removeClone(DBMSDB target, String suffix) {
    }

    public void analyzeDatabase(IDatabase database) {
    }

    public void removeTable(String tableName, IDatabase deltaDB) {
    }
}
