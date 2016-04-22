package speedy.model.database.operators.dbms;

import java.sql.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.IDatabase;
import speedy.model.database.dbms.DBMSTupleIterator;
import speedy.model.database.dbms.SQLQueryString;
import speedy.persistence.relational.AccessConfiguration;
import speedy.persistence.relational.QueryManager;
import speedy.utility.DBMSUtility;

public class RunSQLQueryString {

    private final static Logger logger = LoggerFactory.getLogger(RunSQLQueryString.class);

    public ITupleIterator runQuery(SQLQueryString sqlQuery, IDatabase database) {
        AccessConfiguration accessConfiguration = DBMSUtility.getAccessConfiguration(database);
        if (logger.isDebugEnabled()) logger.debug("Executing sql \n" + sqlQuery);
        ResultSet resultSet = QueryManager.executeQuery(sqlQuery.getQuery(), accessConfiguration);
        return new DBMSTupleIterator(resultSet);
    }

}
