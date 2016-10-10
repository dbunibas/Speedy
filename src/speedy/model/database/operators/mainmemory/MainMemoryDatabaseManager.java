package speedy.model.database.operators.mainmemory;

import speedy.model.database.IDatabase;
import speedy.model.database.mainmemory.MainMemoryDB;
import speedy.model.database.mainmemory.datasource.DataSource;
import speedy.model.database.mainmemory.datasource.INode;
import speedy.model.database.mainmemory.datasource.IntegerOIDGenerator;
import speedy.model.database.mainmemory.datasource.nodes.TupleNode;
import speedy.model.database.operators.IDatabaseManager;
import speedy.persistence.PersistenceConstants;

public class MainMemoryDatabaseManager implements IDatabaseManager {

    public IDatabase createDatabase(IDatabase target, String suffix) {
        INode schemaNode = new TupleNode(PersistenceConstants.DATASOURCE_ROOT_LABEL, IntegerOIDGenerator.getNextOID());
        schemaNode.setRoot(true);
//        generateSchema(schemaNode, (MainMemoryDB) database, affectedAttributes);
        DataSource deltaDataSource = new DataSource(PersistenceConstants.TYPE_META_INSTANCE, schemaNode);
        MainMemoryDB database = new MainMemoryDB(deltaDataSource);
//        generateInstance(database, (MainMemoryDB) database, rootName, affectedAttributes);
        return database;
    }

    public IDatabase cloneTarget(IDatabase target, String suffix) {
        return target.clone();
    }

    public void removeClone(IDatabase target, String suffix) {
    }

    public void removeTable(String tableName, IDatabase deltaDB) {
    }

    public void addUniqueConstraints(IDatabase db) {
    }

    public void initDatabase(IDatabase source, IDatabase target, boolean cleanTarget, boolean preventInsertDuplicateTuples) {
    }
}
