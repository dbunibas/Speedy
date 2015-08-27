package speedy.model.algebra.operators.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.algebra.operators.IBatchInsert;
import speedy.model.database.IDatabase;
import speedy.model.database.ITable;
import speedy.model.database.Tuple;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.database.dbms.DBMSTable;
import speedy.persistence.relational.AccessConfiguration;
import speedy.persistence.relational.QueryManager;

// Thread unsafe
public class SQLBatchInsert implements IBatchInsert {

    private static Logger logger = LoggerFactory.getLogger(SQLBatchInsert.class);
    private Map<ITable, List<Tuple>> buffer = new HashMap<ITable, List<Tuple>>();
    private SQLInsertTuple insertTupleOperator = new SQLInsertTuple();
    private int BUFFER_SIZE = 10000;

    public void insert(ITable table, Tuple tuple, IDatabase database) {
        List<Tuple> tuplesForTable = getTuplesForTable(table);
        tuplesForTable.add(tuple);
        if (tuplesForTable.size() > BUFFER_SIZE) {
            insertTuples(table, tuplesForTable, database);
            buffer.remove(table);
        }
    }

    public void flush(IDatabase database) {
        for (ITable table : buffer.keySet()) {
            List<Tuple> tuplesForTable = getTuplesForTable(table);
            insertTuples(table, tuplesForTable, database);
        }
        buffer.clear();
    }

    private List<Tuple> getTuplesForTable(ITable table) {
        List<Tuple> result = buffer.get(table);
        if (result == null) {
            result = new ArrayList<Tuple>();
            buffer.put(table, result);
        }
        return result;
    }

    private void insertTuples(ITable table, List<Tuple> tuplesForTable, IDatabase database) {
        String tableName = table.getName();
        AccessConfiguration accessConfiguration = ((DBMSDB) database).getAccessConfiguration();
        StringBuilder sb = new StringBuilder();
        for (Tuple tuple : tuplesForTable) {
            sb.append(insertTupleOperator.buildInsertScript((DBMSTable) table, tuple, null, database));
        }
        if (logger.isDebugEnabled()) logger.debug(tuplesForTable.size() + " tuple inserted in table " + tableName);
        QueryManager.executeScript(sb.toString(), accessConfiguration, true, true, false, false);
    }

}
