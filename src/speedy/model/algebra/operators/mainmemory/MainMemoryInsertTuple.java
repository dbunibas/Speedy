package speedy.model.algebra.operators.mainmemory;

import speedy.model.algebra.operators.IInsertTuple;
import speedy.utility.SpeedyUtility;
import speedy.model.database.mainmemory.MainMemoryTable;
import speedy.model.database.mainmemory.datasource.DataSource;
import speedy.model.database.mainmemory.datasource.INode;
import speedy.model.database.mainmemory.datasource.IntegerOIDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.database.Cell;
import speedy.model.database.IDatabase;
import speedy.model.database.ITable;
import speedy.model.database.Tuple;

public class MainMemoryInsertTuple implements IInsertTuple {

    private static Logger logger = LoggerFactory.getLogger(MainMemoryInsertTuple.class);

    @Override
    public void execute(ITable table, Tuple tuple, IDatabase source, IDatabase target) {
        if (logger.isDebugEnabled()) logger.debug("----Executing insert into table " + table.getName() + " tuple: " + tuple);
        DataSource dataSource = ((MainMemoryTable) table).getDataSource();
        String tupleLabel = dataSource.getSchema().getChild(0).getLabel();
        INode tupleNode = SpeedyUtility.createNode("TupleNode", tupleLabel, tuple.getOid());
        dataSource.getInstances().get(0).addChild(tupleNode);
        for (Cell cell : tuple.getCells()) {
            INode attributeNode = SpeedyUtility.createNode("AttributeNode", cell.getAttribute(), IntegerOIDGenerator.getNextOID());
            tupleNode.addChild(attributeNode);
            String leafLabel = dataSource.getSchema().getChild(0).getChild(cell.getAttribute()).getChild(0).getLabel();
            INode leafNode = SpeedyUtility.createNode("LeafNode", leafLabel, cell.getValue());
            attributeNode.addChild(leafNode);
        }
    }
}
