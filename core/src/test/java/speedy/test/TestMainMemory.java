package speedy.test;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.OperatorFactory;
import speedy.model.algebra.Scan;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.TableAlias;
import speedy.model.database.mainmemory.MainMemoryDB;
import speedy.model.database.operators.IRunQuery;
import speedy.persistence.DAOMainMemoryDatabase;
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
}
