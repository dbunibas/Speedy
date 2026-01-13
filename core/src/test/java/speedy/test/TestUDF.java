package speedy.test;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import speedy.model.algebra.Scan;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.algebra.operators.ListTupleIterator;
import speedy.model.algebra.udf.IUserDefinedFunction;
import speedy.model.algebra.udf.UserDefinedFunction;
import speedy.model.database.AttributeRef;
import speedy.model.database.TableAlias;
import speedy.model.database.Tuple;
import speedy.model.database.mainmemory.MainMemoryDB;
import speedy.persistence.DAOMainMemoryDatabase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

public class TestUDF {
    private static final String TABLE_NAME = "EmpTable";

    private MainMemoryDB db;

    @Before
    public void setUp() {
        String schema = requireNonNull(TestUDF.class.getResource("/employees/mainmemory/schema.xsd")).getFile();
        String instance = requireNonNull(TestUDF.class.getResource("/employees/mainmemory/50_emp.xml")).getFile();
        db = new DAOMainMemoryDatabase().loadXMLDatabase(schema, instance);
    }

    @Test
    public void testMockUDF() {
        TableAlias tableAlias = new TableAlias(TABLE_NAME);

        Scan scan = new Scan(tableAlias);

        UserDefinedFunction udf = new UserDefinedFunction(new IUserDefinedFunction() {
            @Override
            public ITupleIterator execute(ITupleIterator iterator) {
                return iterator;
            }
        });
        udf.addChild(scan);

        ITupleIterator iterator = udf.execute(db, db);
        List<Tuple> tuples = toTupleList(iterator);

        Assert.assertEquals(50, tuples.size());
    }

    @Test
    public void testSalaryFilter() {
        TableAlias tableAlias = new TableAlias(TABLE_NAME);

        Scan scan = new Scan(tableAlias);

        UserDefinedFunction udf = new UserDefinedFunction(new SalaryFilter(100000));
        udf.addChild(scan);

        ITupleIterator iterator = udf.execute(db, db);
        List<Tuple> tuples = toTupleList(iterator);

        Assert.assertEquals(1, tuples.size());
    }

    private List<Tuple> toTupleList(ITupleIterator iterator) {
        Iterable<Tuple> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false).toList();
    }

    private static final class SalaryFilter implements IUserDefinedFunction {
        private int minimumSalary;

        public SalaryFilter(int minimumSalary) {
            this.minimumSalary = minimumSalary;
        }

        @Override
        public ITupleIterator execute(ITupleIterator iterator) {
            AttributeRef attributeRef = new AttributeRef(TABLE_NAME, "salary");

            List<Tuple> tuples = new ArrayList<>();
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                Integer salary = (Integer) tuple.getCell(attributeRef).getValue().getPrimitiveValue();
                if (salary >= minimumSalary) tuples.add(tuple);
            }

            iterator.close();
            return new ListTupleIterator(tuples);
        }
    }
}
