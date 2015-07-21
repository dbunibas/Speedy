package speedy.model.database.operators;

import speedy.model.database.IDatabase;
import speedy.model.database.dbms.DBMSDB;

public interface IDatabaseManager {

    public IDatabase cloneTarget(DBMSDB target, String suffix);

    public void removeClone(DBMSDB target, String suffix);

    public void analyzeDatabase(IDatabase database);

    public void removeTable(String tableName, IDatabase deltaDB);

}
