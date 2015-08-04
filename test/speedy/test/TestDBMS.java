package speedy.test;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.OperatorFactory;
import speedy.model.algebra.Join;
import speedy.model.algebra.Scan;
import speedy.model.algebra.Select;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.AttributeRef;
import speedy.model.database.TableAlias;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.database.operators.IRunQuery;
import speedy.model.expressions.Expression;
import speedy.persistence.DAODBMSDatabase;
import speedy.persistence.relational.QueryStatManager;
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
        database = daoDatabase.loadDatabase(driver, uri, schema, login, password);
        database.getInitDBConfiguration().setCreateTablesFromFiles(true);
        database.getInitDBConfiguration().addFileToImportForTable("emp", UtilityForTests.getAbsoluteFileName("/resources/employees/xml/50_emp.xml"));
        UtilityForTests.deleteDB(database.getAccessConfiguration());
        queryRunner = OperatorFactory.getInstance().getQueryRunner(database);
    }

    @Test
    public void testScan() {
        TableAlias tableAlias = new TableAlias("emp");
        Scan scan = new Scan(tableAlias);
        if (logger.isDebugEnabled()) logger.debug(scan.toString());
        ITupleIterator result = queryRunner.run(scan, null, database);
        String stringResult = SpeedyUtility.printTupleIterator(result);
        if (logger.isDebugEnabled()) logger.debug(stringResult);
        result.close();
        Assert.assertTrue(stringResult.startsWith("Number of tuples: 50\n"));
    }

    @Test
    public void testSelect() {
        TableAlias tableAlias = new TableAlias("emp");
        Scan scan = new Scan(tableAlias);
        Expression expression = new Expression("name == \"Paolo\"");
        expression.changeVariableDescription("name", new AttributeRef(tableAlias, "name"));
        Select select = new Select(expression);
        select.addChild(scan);
        if (logger.isDebugEnabled()) logger.debug(select.toString());
        ITupleIterator result = queryRunner.run(select, null, database);
        String stringResult = SpeedyUtility.printTupleIterator(result);
        if (logger.isDebugEnabled()) logger.debug(stringResult);
        result.close();
        Assert.assertTrue(stringResult.startsWith("Number of tuples: 1\n"));
        QueryStatManager.getInstance().printStatistics();
    }

    @Test
    public void testJoin() {
        TableAlias tableAlias1 = new TableAlias("emp", "1");
        Scan scan1 = new Scan(tableAlias1);
        TableAlias tableAlias2 = new TableAlias("emp", "2");
        Scan scan2 = new Scan(tableAlias2);
        Join join = new Join(new AttributeRef(tableAlias1, "name"), new AttributeRef(tableAlias2, "manager"));
        join.addChild(scan1);
        join.addChild(scan2);
        if (logger.isDebugEnabled()) logger.debug(join.toString());
        ITupleIterator result = queryRunner.run(join, null, database);
        String stringResult = SpeedyUtility.printTupleIterator(result);
        if (logger.isDebugEnabled()) logger.debug(stringResult);
        result.close();
        Assert.assertTrue(stringResult.startsWith("Number of tuples: 2\n"));
        QueryStatManager.getInstance().printStatistics();
    }
}
