package speedy.model.database.operators.mainmemory;

import speedy.model.database.operators.IRunQuery;
import speedy.model.algebra.IAlgebraOperator;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.IDatabase;
import speedy.model.database.ResultInfo;
import speedy.model.database.Tuple;
import speedy.model.database.TupleOID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainMemoryRunQuery implements IRunQuery {

    private static Logger logger = LoggerFactory.getLogger(MainMemoryRunQuery.class);

    public ITupleIterator run(IAlgebraOperator query, IDatabase source, IDatabase target) {
        return query.execute(source, target);
    }

    public ResultInfo getSize(IAlgebraOperator query, IDatabase source, IDatabase target) {
        ITupleIterator iterator = this.run(query, source, target);
        long count = 0;
        long minOid = Long.MAX_VALUE;
        long maxOid = 0;
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            count++;
            TupleOID oidIValue = tuple.getOid();
            long oidLongValue = Long.parseLong(oidIValue.toString());
            if (oidLongValue > maxOid) {
                maxOid = oidLongValue;
            }
            if (oidLongValue < minOid) {
                minOid = oidLongValue;
            }
        }
        if (count == 0) {
            minOid = 0;
        }
        iterator.close();
        ResultInfo result = new ResultInfo(count);
        result.setMinOid(minOid);
        result.setMaxOid(maxOid);
        return result;
    }

}
