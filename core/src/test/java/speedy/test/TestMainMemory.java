package speedy.test;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.OperatorFactory;
import speedy.model.algebra.Intersection;
import speedy.model.algebra.Scan;
import speedy.model.algebra.Select;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.AttributeRef;
import speedy.model.database.TableAlias;
import speedy.model.database.mainmemory.MainMemoryDB;
import speedy.model.database.operators.IRunQuery;
import speedy.model.expressions.Expression;
import speedy.persistence.DAOMainMemoryDatabase;
import speedy.persistence.relational.QueryStatManager;
import speedy.test.utility.UtilityForTests;
import speedy.utility.SpeedyUtility;

public class TestMainMemory {

    private static Logger logger = LoggerFactory.getLogger(TestMainMemory.class);

    private MainMemoryDB database;
    private IRunQuery queryRunner;

    @Before
    public void setUp() {
        DAOMainMemoryDatabase daoDatabase = new DAOMainMemoryDatabase();
        String xsdSchema = "/employees/mainmemory/schema.xsd";
        String xmlInstance = "/employees/mainmemory/50_emp.xml";
//        String xsdSchema = "/bookpublisher/bookPublisher-schema.xsd";
//        String xmlInstance = "/bookpublisher/bookPublisher-instance.xml";
        database = daoDatabase.loadXMLDatabase(UtilityForTests.getAbsoluteFileName(xsdSchema), UtilityForTests.getAbsoluteFileName(xmlInstance));
        queryRunner = OperatorFactory.getInstance().getQueryRunner(database);
    }

    @Test
    public void testScan() {
        TableAlias tableAlias = new TableAlias("EmpTable");
        Scan scan = new Scan(tableAlias);
        if (logger.isDebugEnabled()) logger.debug(scan.toString());
        ITupleIterator result = queryRunner.run(scan, null, database);
        String stringResult = SpeedyUtility.printTupleIterator(result);
        if (logger.isDebugEnabled()) logger.debug(stringResult);
        result.close();
        Assert.assertTrue(stringResult.startsWith("Number of tuples: 50\n"));
    }

    @Test
    public void testIntersection1() {
        TableAlias tableAlias = new TableAlias("EmpTable");
        Scan scan = new Scan(tableAlias);
        Expression expression = new Expression("salary > 3000");
        expression.changeVariableDescription("salary", new AttributeRef(tableAlias, "salary"));
        Select select1 = new Select(expression);
        select1.addChild(scan);

        Expression expression2 = new Expression("salary > 2000");
        expression2.changeVariableDescription("salary", new AttributeRef(tableAlias, "salary"));
        Select select2 = new Select(expression2);
        select2.addChild(scan);

        Intersection intersection = new Intersection();
        intersection.addChild(select1);
        intersection.addChild(select2);
        if (logger.isDebugEnabled()) logger.debug(intersection.toString());
        ITupleIterator result = queryRunner.run(intersection, null, database);
        String stringResult = SpeedyUtility.printTupleIterator(result);
        if (logger.isDebugEnabled()) logger.debug(stringResult);
        result.close();
        Assert.assertTrue(stringResult.startsWith("Number of tuples: 24\n"));
        QueryStatManager.getInstance().printStatistics();
    }
    
    @Test
    public void testIntersection2() {
        TableAlias tableAlias = new TableAlias("EmpTable");
        Scan scan = new Scan(tableAlias);
        Expression expression = new Expression("salary < 2000");
        expression.changeVariableDescription("salary", new AttributeRef(tableAlias, "salary"));
        Select select1 = new Select(expression);
        select1.addChild(scan);

        Expression expression2 = new Expression("manager == \"Paolo\"");
        expression2.changeVariableDescription("manager", new AttributeRef(tableAlias, "manager"));
        Select select2 = new Select(expression2);
        select2.addChild(scan);

        Intersection intersection = new Intersection();
        intersection.addChild(select1);
        intersection.addChild(select2);
        if (logger.isDebugEnabled()) logger.debug(intersection.toString());
        ITupleIterator result = queryRunner.run(intersection, null, database);
        String stringResult = SpeedyUtility.printTupleIterator(result);
        if (logger.isDebugEnabled()) logger.debug(stringResult);
        result.close();
        Assert.assertTrue(stringResult.startsWith("Number of tuples: 1\n"));
        QueryStatManager.getInstance().printStatistics();
    }
}
