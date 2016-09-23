package speedy.model.database.operators;

import speedy.model.database.IDatabase;
import speedy.model.database.dbms.DBMSDB;

public interface IDatabaseManager {

    public IDatabase createDatabase(IDatabase target, String suffix);
    
    public IDatabase cloneTarget(IDatabase target, String suffix);

    public void removeClone(IDatabase target, String suffix);

    public void analyzeDatabase(IDatabase database);

    public void removeTable(String tableName, IDatabase deltaDB);
    
    public void addUniqueConstraints(IDatabase db);

}
