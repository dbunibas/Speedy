package speedy;

import speedy.model.database.operators.IRunQuery;
import speedy.model.database.operators.dbms.SQLRunQuery;
import speedy.model.database.operators.mainmemory.MainMemoryRunQuery;
import speedy.model.algebra.operators.IInsertTuple;
import speedy.model.algebra.operators.IUpdateCell;
import speedy.model.algebra.operators.mainmemory.InsertTuple;
import speedy.model.algebra.operators.mainmemory.UpdateCell;
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
    private IUpdateCell mainMemoryCellUpdater = new UpdateCell();
    private IUpdateCell sqlCellUpdater = new SQLUpdateCell();
    //
    private IInsertTuple mainMemoryInsertOperator = new InsertTuple();
    private IInsertTuple sqlInsertOperator = new SQLInsertTuple();
    //
    private IDatabaseManager mainMemoryDatabaseManager = new MainMemoryDatabaseManager();
    private IDatabaseManager sqlDatabaseManager = new SQLDatabaseManager();

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

    private boolean isMainMemory(IDatabase database) {
        return (database instanceof MainMemoryDB);
    }
}
