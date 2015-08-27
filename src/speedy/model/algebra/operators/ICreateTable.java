package speedy.model.algebra.operators;

import java.util.List;
import speedy.model.database.Attribute;
import speedy.model.database.IDatabase;

public interface ICreateTable {

    public void createTable(String tableName, List<Attribute> attributes, IDatabase target);
}
