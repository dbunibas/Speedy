package speedy.test;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.Attribute;
import speedy.model.database.AttributeRef;
import speedy.model.database.Cell;
import speedy.model.database.ConstantValue;
import speedy.model.database.ITable;
import speedy.model.database.LLUNValue;
import speedy.model.database.NullValue;
import speedy.model.database.Tuple;
import speedy.model.database.mainmemory.MainMemoryDB;
import speedy.persistence.DAOMainMemoryDatabase;
import speedy.utility.SpeedyUtility;
import speedy.utility.test.UtilityForTests;

public class TestMainMemoryCSV {

    private static final Logger logger = LoggerFactory.getLogger(TestMainMemoryCSV.class);

    private MainMemoryDB database;
    private DAOMainMemoryDatabase dao = new DAOMainMemoryDatabase();

    @Before
    public void setUp() {
        String folder = UtilityForTests.getAbsoluteFileName("/resources/homomorphism/");
        database = dao.loadCSVDatabase(folder, ',', null);
    }

    @Test
    public void testDB() {
//        System.out.println("----- INSTANCES --------");
//        System.out.println(database.printInstances());
//        System.out.println("------------------------");
        ITable table = database.getTable("02-source");
        Assert.assertTrue(table.getSize() == 4);
        Assert.assertTrue(table.getAttributes().size() == 4);
        ITupleIterator tupleIterator = table.getTupleIterator();
        Attribute attributeA = table.getAttribute("A");
        Attribute attributeC = table.getAttribute("C");
        Assert.assertNotNull(attributeA);
        Assert.assertNotNull(attributeC);
        Tuple firstTuple = tupleIterator.next();
        AttributeRef attributeRefA = new AttributeRef("02-source", "A");
        AttributeRef attributeRefC = new AttributeRef("02-source", "C");
        Cell cell1 = firstTuple.getCell(attributeRefA);
        Cell cellNull5 = firstTuple.getCell(attributeRefC);
        Assert.assertNotNull(cell1);
        Assert.assertNotNull(cellNull5);
        Assert.assertTrue(cell1.getValue().toString().equals("1"));
        Assert.assertTrue(cell1.getValue() instanceof ConstantValue);
        Assert.assertTrue(cellNull5.getValue().toString().equals("_N5"));
        Assert.assertTrue(cellNull5.getValue() instanceof NullValue);
        Assert.assertTrue(SpeedyUtility.isNullValue(cellNull5.getValue()));
        tupleIterator.close();
        ITable table2 = database.getTable("03-source");
        ITupleIterator tupleIterator2 = table2.getTupleIterator();
        AttributeRef attributeRefA2 = new AttributeRef("03-source", "A");
        AttributeRef attributeRefB2 = new AttributeRef("03-source", "B");
        AttributeRef attributeRefC2 = new AttributeRef("03-source", "C");
        AttributeRef attributeRefD2 = new AttributeRef("03-source", "D");
        Tuple tuple = tupleIterator2.next();
        Cell cellA = tuple.getCell(attributeRefA2);
        Cell cellB = tuple.getCell(attributeRefB2);
        Cell cellC = tuple.getCell(attributeRefC2);
        Cell cellD = tuple.getCell(attributeRefD2);
        Assert.assertTrue(cellA.getValue() instanceof ConstantValue);
        Assert.assertTrue(cellB.getValue() instanceof NullValue);
        Assert.assertTrue(cellC.getValue() instanceof NullValue);
        Assert.assertTrue(cellD.getValue() instanceof LLUNValue);
        tupleIterator2.close();
    }

}
