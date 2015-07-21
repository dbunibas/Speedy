package speedy.model.database.dbms;

import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.Tuple;
import speedy.exceptions.DBMSException;
import speedy.utility.DBMSUtility;
import speedy.persistence.relational.QueryManager;
import speedy.persistence.relational.QueryStatManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBMSTupleIterator implements ITupleIterator {

    private ResultSet resultSet;
    private String tableName;
    private boolean empty;
    private boolean firstTupleRead;

    public DBMSTupleIterator(ResultSet resultSet) {
        this(resultSet, null);
    }

    public DBMSTupleIterator(ResultSet resultSet, String tableName) {
        this.resultSet = resultSet;
        this.tableName = tableName;
        try {
            firstTupleRead = moveResultSet(resultSet);
            if (!firstTupleRead) {
                empty = true;
            }
//            resultSet.last();
//            int size = resultSet.getRow();
//            resultSet.beforeFirst();
//            this.empty = (size == 0);
        } catch (SQLException ex) {
            throw new DBMSException("Exception in running result set:" + ex);
        }
    }

    public boolean hasNext() {
        try {
//            return !empty && !resultSet.isLast();
            if (empty) {
                return false;
            }
            if (firstTupleRead) {
                return true;
            }
            return !resultSet.isLast();
        } catch (SQLException ex) {
            throw new DBMSException("Exception in running result set:" + ex);
        }
    }

    public Tuple next() {
        try {
            if (firstTupleRead) {
                firstTupleRead = false;
            } else {
                moveResultSet(resultSet);
            }
            Tuple tuple = DBMSUtility.createTuple(resultSet, tableName);
            return tuple;
        } catch (SQLException ex) {
            throw new DBMSException("Exception in running result set:" + ex);
        }
    }

    public void reset() {
        throw new UnsupportedOperationException("Unable to reset DBMS result set");
//        try {
//            resultSet.beforeFirst();
//        } catch (SQLException ex) {
//            throw new DBMSException("Exception in running result set:" + ex);
//        }
    }

    public void remove() {
        throw new UnsupportedOperationException("Not supported.");
    }

    public void close() {
        QueryManager.closeResultSet(resultSet);
    }

    private boolean moveResultSet(ResultSet resultSet) throws SQLException {
        QueryStatManager.getInstance().addReadTuple();
        return resultSet.next();
    }
}
