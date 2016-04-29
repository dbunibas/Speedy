package speedy.test;

import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.OperatorFactory;
import speedy.SpeedyConstants;
import speedy.model.algebra.GroupBy;
import speedy.model.algebra.Join;
import speedy.model.algebra.Limit;
import speedy.model.algebra.OrderBy;
import speedy.model.algebra.Project;
import speedy.model.algebra.Scan;
import speedy.model.algebra.Select;
import speedy.model.algebra.aggregatefunctions.CountAggregateFunction;
import speedy.model.algebra.aggregatefunctions.IAggregateFunction;
import speedy.model.algebra.aggregatefunctions.ValueAggregateFunction;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.AttributeRef;
import speedy.model.database.TableAlias;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.database.operators.IRunQuery;
import speedy.model.expressions.Expression;
import speedy.persistence.DAODBMSDatabase;
import speedy.persistence.file.CSVFile;
import speedy.utility.test.UtilityForTests;
import speedy.utility.SpeedyUtility;

public class TestDBMSCSV {

    private static Logger logger = LoggerFactory.getLogger(TestDBMSCSV.class);

    private DBMSDB database;
    private IRunQuery queryRunner;

    @Before
    public void setUp() {
        DAODBMSDatabase daoDatabase = new DAODBMSDatabase();
        String driver = "org.postgresql.Driver";
        String uri = "jdbc:postgresql:speedy_tpch_dbgen_1k";
        String schema = "target";
        String login = "pguser";
        String password = "pguser";
        database = daoDatabase.loadDatabase(driver, uri, schema, login, password);
        database.getInitDBConfiguration().setCreateTablesFromFiles(true);
        CSVFile fileToImport = new CSVFile(UtilityForTests.getAbsoluteFileName("/resources/employees/csv/50_emp.csv"));
        fileToImport.setSeparator(',');
//        database.getInitDBConfiguration().addFileToImportForTable("emp", fileToImport);
//        UtilityForTests.deleteDB(database.getAccessConfiguration());
        queryRunner = OperatorFactory.getInstance().getQueryRunner(database);
    }

    @After
    public void tearDown() {
//        UtilityForTests.deleteDB(database.getAccessConfiguration());
    }

    @Test
    public void xtestOrderByGroupBySelect() {
        TableAlias tableAlias = new TableAlias("part");
        Scan scan = new Scan(tableAlias);
        AttributeRef attributeRef = new AttributeRef(tableAlias, "p_name");
        Expression expression = new Expression("p_size > 20"); //
        expression.changeVariableDescription("p_size", new AttributeRef(tableAlias, "p_size")); //
        Select select = new Select(expression); //
        select.addChild(scan); //
        List<AttributeRef> groupingAttribute = new ArrayList<AttributeRef>();
        groupingAttribute.add(attributeRef);
        List<IAggregateFunction> aggregateFunctions = new ArrayList<IAggregateFunction>();
        aggregateFunctions.add(new ValueAggregateFunction(attributeRef));
        AttributeRef attributeCount = new AttributeRef(tableAlias, SpeedyConstants.COUNT);
        CountAggregateFunction countAggregateFunction = new CountAggregateFunction(attributeCount);
        aggregateFunctions.add(countAggregateFunction);
        GroupBy groupBy = new GroupBy(groupingAttribute, aggregateFunctions);
        groupBy.addChild(select);
        List<AttributeRef> countAttributeRef = new ArrayList<AttributeRef>();
        countAttributeRef.add(attributeCount);
        OrderBy orderBy = new OrderBy(countAttributeRef);
        orderBy.setOrder(OrderBy.ORDER_DESC);
        orderBy.addChild(groupBy);
        Limit limit = new Limit(10);
        limit.addChild(orderBy);
        if (logger.isDebugEnabled()) logger.debug(limit.toString());
        ITupleIterator result = queryRunner.run(limit, null, database);
        String stringResult = SpeedyUtility.printTupleIterator(result);
        if (logger.isDebugEnabled()) logger.debug(stringResult);
        result.close();
        Assert.assertTrue(stringResult.startsWith("Number of tuples: 10\n"));
    }
    @Test
    public void testOrderByGroupBy() {
        TableAlias tableAlias = new TableAlias("part");
        Scan scan = new Scan(tableAlias);
        AttributeRef attributeRef = new AttributeRef(tableAlias, "p_name");
        
        List<AttributeRef> groupingAttribute = new ArrayList<AttributeRef>();
        groupingAttribute.add(attributeRef);
        List<IAggregateFunction> aggregateFunctions = new ArrayList<IAggregateFunction>();
        aggregateFunctions.add(new ValueAggregateFunction(attributeRef));
        AttributeRef attributeCount = new AttributeRef(tableAlias, SpeedyConstants.COUNT);
        CountAggregateFunction countAggregateFunction = new CountAggregateFunction(attributeCount);
        aggregateFunctions.add(countAggregateFunction);
        GroupBy groupBy = new GroupBy(groupingAttribute, aggregateFunctions);
        groupBy.addChild(scan);
        List<AttributeRef> countAttributeRef = new ArrayList<AttributeRef>();
        countAttributeRef.add(attributeCount);
        OrderBy orderBy = new OrderBy(countAttributeRef);
        orderBy.setOrder(OrderBy.ORDER_DESC);
        orderBy.addChild(groupBy);
        Limit limit = new Limit(10);
        limit.addChild(orderBy);
        if (logger.isDebugEnabled()) logger.debug(limit.toString());
        ITupleIterator result = queryRunner.run(limit, null, database);
        String stringResult = SpeedyUtility.printTupleIterator(result);
        if (logger.isDebugEnabled()) logger.debug(stringResult);
        result.close();
        Assert.assertTrue(stringResult.startsWith("Number of tuples: 10\n"));
    }
    @Test
    public void xtestOrderByGroupByWithJoinSelect() {
        TableAlias tableAliasPart = new TableAlias("part");
        TableAlias tableAliasPartSupp = new TableAlias("partsupp");
        Scan scanPart = new Scan(tableAliasPart);
        Scan scanPartSupp = new Scan(tableAliasPartSupp);
        Expression expression = new Expression("p_size > 20"); //
        expression.changeVariableDescription("p_size", new AttributeRef(tableAliasPart, "p_size")); //
        Select select = new Select(expression); //
        select.addChild(scanPart); //
        List<AttributeRef> leftAttributes = new ArrayList<AttributeRef>();
        leftAttributes.add(new AttributeRef(tableAliasPart, "p_partkey"));
        List<AttributeRef> rightAttributes = new ArrayList<AttributeRef>();
        rightAttributes.add(new AttributeRef(tableAliasPartSupp, "ps_partkey"));
        Join join = new Join(leftAttributes, rightAttributes);
        join.addChild(select);
        join.addChild(scanPartSupp);
        AttributeRef attributeRef = new AttributeRef(tableAliasPart, "p_name");
        List<AttributeRef> groupingAttribute = new ArrayList<AttributeRef>();
        groupingAttribute.add(attributeRef);
        List<IAggregateFunction> aggregateFunctions = new ArrayList<IAggregateFunction>();
        aggregateFunctions.add(new ValueAggregateFunction(attributeRef));
        AttributeRef attributeCount = new AttributeRef(tableAliasPart, SpeedyConstants.COUNT);
        CountAggregateFunction countAggregateFunction = new CountAggregateFunction(attributeCount);
        aggregateFunctions.add(countAggregateFunction);
        GroupBy groupBy = new GroupBy(groupingAttribute, aggregateFunctions);
        groupBy.addChild(join);
        List<AttributeRef> countAttributeRef = new ArrayList<AttributeRef>();
        countAttributeRef.add(attributeCount);
        OrderBy orderBy = new OrderBy(countAttributeRef);
        orderBy.setOrder(OrderBy.ORDER_DESC);
        orderBy.addChild(groupBy);
        Limit limit = new Limit(10);
        limit.addChild(orderBy);
        if (logger.isDebugEnabled()) {
            logger.debug(limit.toString());
        }
        ITupleIterator result = queryRunner.run(limit, null, database);
        String stringResult = SpeedyUtility.printTupleIterator(result);
        if (logger.isDebugEnabled()) {
            logger.debug(stringResult);
        }
        result.close();
        Assert.assertTrue(stringResult.startsWith("Number of tuples: 10\n"));
    }

//    @Test
//    public void testScan() {
//        TableAlias tableAlias = new TableAlias("emp");
//        Scan scan = new Scan(tableAlias);
//        if (logger.isDebugEnabled()) logger.debug(scan.toString());
//        ITupleIterator result = queryRunner.run(scan, null, database);
//        String stringResult = SpeedyUtility.printTupleIterator(result);
//        if (logger.isDebugEnabled()) logger.debug(stringResult);
//        result.close();
//        Assert.assertTrue(stringResult.startsWith("Number of tuples: 50\n"));
//    }
//
//    @Test
//    public void testSelect() {
//        TableAlias tableAlias = new TableAlias("emp");
//        Scan scan = new Scan(tableAlias);
//        Expression expression = new Expression("salary > 3000");
//        expression.changeVariableDescription("salary", new AttributeRef(tableAlias, "salary"));
//        Select select = new Select(expression);
//        select.addChild(scan);
//        if (logger.isDebugEnabled()) logger.debug(select.toString());
//        ITupleIterator result = queryRunner.run(select, null, database);
//        String stringResult = SpeedyUtility.printTupleIterator(result);
//        if (logger.isDebugEnabled()) logger.debug(stringResult);
//        result.close();
//        Assert.assertTrue(stringResult.startsWith("Number of tuples: 24\n"));
//        QueryStatManager.getInstance().printStatistics();
//    }
//    @Test
//    public void testRandom() {
//        TableAlias tableAlias = new TableAlias("emp");
//        Scan scan = new Scan(tableAlias);
//        OrderByRandom random = new OrderByRandom();
//        random.addChild(scan);
//        Limit limit = new Limit(10);
//        limit.addChild(random);
//        if (logger.isDebugEnabled()) logger.debug(limit.toString());
//        ITupleIterator result = queryRunner.run(limit, null, database);
//        String stringResult = SpeedyUtility.printTupleIterator(result);
//        if (logger.isDebugEnabled()) logger.debug(stringResult);
//        result.close();
//        Assert.assertTrue(stringResult.startsWith("Number of tuples: 10\n"));
//    }
}
