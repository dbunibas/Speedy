package speedy.model.algebra.operators.mainmemory;

import java.util.List;
import speedy.model.algebra.operators.ICreateTable;
import speedy.model.database.Attribute;
import speedy.model.database.IDatabase;

public class MainMemoryCreateTable implements ICreateTable {

    public void createTable(String tableName, List<Attribute> attributes, IDatabase target) {
        throw new UnsupportedOperationException("Not supported yet."); //TODO Implement method
    }

}
