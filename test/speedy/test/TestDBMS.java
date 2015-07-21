package speedy.test;

import java.util.Iterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.OperatorFactory;
import speedy.model.algebra.Scan;
import speedy.model.database.IDatabase;
import speedy.model.database.TableAlias;
import speedy.model.database.Tuple;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.database.operators.IRunQuery;
import speedy.persistence.DAODBMSDatabase;
import speedy.test.utility.UtilityForTests;
import speedy.utility.SpeedyUtility;

public class TestDBMS {

    private static Logger logger = LoggerFactory.getLogger(TestDBMS.class);

    private DBMSDB database;
    private IRunQuery queryRunner;

    @Before
    public void setUp() {
        DAODBMSDatabase daoDatabase = new DAODBMSDatabase();
        String driver = "org.postgresql.Driver";
        String uri = "jdbc:postgresql:speedy_employees";
        String schema = "target";
        String login = "pguser";
        String password = "pguser";
        database = daoDatabase.loadXMLDatabase(driver, uri, schema, login, password);
        database.getInitDBConfiguration().setCreateTablesFromXML(true);
        database.getInitDBConfiguration().addXmlFileToImport(UtilityForTests.getAbsoluteFileName("/resources/employees/xml/50.xml"));
        queryRunner = OperatorFactory.getInstance().getQueryRunner(database);
    }

    @Test
    public void testScan() {
        TableAlias tableAlias = new TableAlias("emp");
        Scan scan = new Scan(tableAlias);
        if (logger.isDebugEnabled()) logger.debug(scan.toString());
        Iterator<Tuple> result = queryRunner.run(scan, null, database);
        String stringResult = SpeedyUtility.printTupleIterator(result);
        if (logger.isDebugEnabled()) logger.debug(stringResult);
        Assert.assertTrue(stringResult.startsWith("Number of tuples: 50\n"));
    }
}
