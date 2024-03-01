package speedy.model.database.operators.mainmemory;

import speedy.model.algebra.IAlgebraOperator;
import speedy.model.database.operators.IExplainQuery;
import speedy.model.database.operators.IRunQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.database.IDatabase;

public class MainMemoryExplainQuery implements IExplainQuery {

    private static Logger logger = LoggerFactory.getLogger(MainMemoryExplainQuery.class);
    private IRunQuery queryRunner = new MainMemoryRunQuery();

    public long explain(IAlgebraOperator query, IDatabase source, IDatabase target) {
        return queryRunner.getSize(query, source, target).getSize();
    }

}
