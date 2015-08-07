package speedy;

import speedy.model.database.operators.IRunQuery;
import speedy.model.database.operators.dbms.SQLRunQuery;
import speedy.model.database.operators.mainmemory.MainMemoryRunQuery;
import speedy.model.algebra.operators.IInsertTuple;
import speedy.model.algebra.operators.IUpdateCell;
import speedy.model.algebra.operators.mainmemory.MainMemoryInsertTuple;
import speedy.model.algebra.operators.mainmemory.MainMemoryUpdateCell;
import speedy.model.algebra.operators.sql.SQLInsertTuple;
import speedy.model.algebra.operators.sql.SQLUpdateCell;
import speedy.model.database.operators.IDatabaseManager;
import speedy.model.database.operators.IExplainQuery;
import speedy.model.database.operators.dbms.SQLDatabaseManager;
import speedy.model.database.operators.dbms.SQLExplainQuery;
import speedy.model.database.operators.mainmemory.MainMemoryDatabaseManager;
import speedy.model.database.operators.mainmemory.MainMemoryExplainQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.algebra.operators.IBatchInsert;
import speedy.model.algebra.operators.ICreateTable;
import speedy.model.algebra.operators.mainmemory.MainMemoryBatchInsert;
import speedy.model.algebra.operators.mainmemory.MainMemoryCreateTable;
import speedy.model.algebra.operators.sql.SQLBatchInsert;
import speedy.model.algebra.operators.sql.SQLCreateTable;
import speedy.model.database.IDatabase;
import speedy.model.database.mainmemory.MainMemoryDB;

public class OperatorFactory {

    private static Logger logger = LoggerFactory.getLogger(OperatorFactory.class);
    private static OperatorFactory singleton = new OperatorFactory();
    //
    private IRunQuery mainMemoryQueryRunner = new MainMemoryRunQuery();
    private IRunQuery sqlQueryRunner = new SQLRunQuery();
    //
    private IExplainQuery mainMemoryQueryExplanator = new MainMemoryExplainQuery();
    private IExplainQuery sqlQueryExplanator = new SQLExplainQuery();
    //
    private IUpdateCell mainMemoryCellUpdater = new MainMemoryUpdateCell();
    private IUpdateCell sqlCellUpdater = new SQLUpdateCell();
    //
    private IInsertTuple mainMemoryInsertOperator = new MainMemoryInsertTuple();
    private IInsertTuple sqlInsertOperator = new SQLInsertTuple();
    //
    private IDatabaseManager mainMemoryDatabaseManager = new MainMemoryDatabaseManager();
    private IDatabaseManager sqlDatabaseManager = new SQLDatabaseManager();
    //
    private ICreateTable mainMemoryTableCreator = new MainMemoryCreateTable();
    private ICreateTable sqlTableCreator = new SQLCreateTable();
    //
    private IBatchInsert mainMemoryBatchInsertOperator = new MainMemoryBatchInsert();
    private IBatchInsert sqlBatchInsertOperator  = new SQLBatchInsert();

    private OperatorFactory() {
    }

    public static OperatorFactory getInstance() {
        return singleton;
    }

    public IRunQuery getQueryRunner(IDatabase database) {
        if (this.isMainMemory(database)) {
            return mainMemoryQueryRunner;
        }
        return sqlQueryRunner;
    }

    public IUpdateCell getCellUpdater(IDatabase database) {
        if (this.isMainMemory(database)) {
            return mainMemoryCellUpdater;
        }
        return sqlCellUpdater;
    }

    public IExplainQuery getQueryExplanator(IDatabase database) {
        if (this.isMainMemory(database)) {
            return mainMemoryQueryExplanator;
        }
        return sqlQueryExplanator;
    }

    public IDatabaseManager getDatabaseManager(IDatabase database) {
        if (this.isMainMemory(database)) {
            return mainMemoryDatabaseManager;
        }
        return sqlDatabaseManager;
    }

    public IInsertTuple getInsertOperator(IDatabase database) {
        if (this.isMainMemory(database)) {
            return mainMemoryInsertOperator;
        }
        return sqlInsertOperator;
    }

    public IBatchInsert getSingletonBatchInsertOperator(IDatabase database) {
        if (this.isMainMemory(database)) {
            return mainMemoryBatchInsertOperator;
        }
        return sqlBatchInsertOperator;
    }

    public IBatchInsert getNonSingletonBatchInsertOperator(IDatabase database) {
        if (this.isMainMemory(database)) {
            return new MainMemoryBatchInsert();
        }
        return new SQLBatchInsert();
    }

    private boolean isMainMemory(IDatabase database) {
        return (database instanceof MainMemoryDB);
    }

    public ICreateTable getTableCreator(IDatabase database) {
        if (this.isMainMemory(database)) {
            return mainMemoryTableCreator;
        }
        return sqlTableCreator;
    }
}
