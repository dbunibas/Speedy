package speedy.model.algebra.operators.mainmemory;

import speedy.model.algebra.IAlgebraOperator;
import speedy.model.algebra.operators.IDelete;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.mainmemory.MainMemoryDB;
import speedy.model.database.mainmemory.datasource.INode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.database.IDatabase;
import speedy.model.database.Tuple;
import speedy.model.database.TupleOID;

public class MainMemoryDelete implements IDelete {

    private static Logger logger = LoggerFactory.getLogger(MainMemoryDelete.class);

    @Override
    public boolean execute(String tableName, IAlgebraOperator sourceQuery, IDatabase source, IDatabase target) {
        if (logger.isDebugEnabled()) logger.debug("----Executing delete.");
        if (logger.isDebugEnabled()) logger.debug("----Source query: " + sourceQuery);
        List<TupleOID> oidsToDelete = new ArrayList<TupleOID>();
        ITupleIterator it = sourceQuery.execute(source, target);
        while (it.hasNext()) {
            Tuple tuple = it.next();
            oidsToDelete.add(tuple.getOid());
//            for (Cell cell : tuple.getCells()) {
//                IValue cellValue = cell.getValue();
//                if (cellValue instanceof NullValue) {
//                    occurrenceHandler.removeOccurrenceForNull(target, (NullValue) cellValue, new CellRef(cell));
//                }
//                if (cellValue instanceof LLUNValue) {
//                    occurrenceHandler.removeOccurrenceForLLUN(target, (LLUNValue) cellValue, new CellRef(cell));
//                }
//            }
        }
        it.close();
        boolean deletedTuples = deleteTuples(tableName, oidsToDelete, target);
        return deletedTuples;
    }

    private boolean deleteTuples(String tableName, List<TupleOID> oidsToDelete, IDatabase database) {
        boolean deletions = false;
        INode instanceRoot = ((MainMemoryDB) database).getDataSource().getInstances().get(0);
        for (INode set : instanceRoot.getChildren()) {
            if (!set.getLabel().equals(tableName)) {
                continue;
            }
            for (Iterator<INode> it = set.getChildren().iterator(); it.hasNext();) {
                INode tuple = it.next();
                if (!isToDelete(tuple.getValue(), oidsToDelete)) {
                    continue;
                }
                it.remove();
                deletions = true;
            }
        }
        return deletions;
    }

    private boolean isToDelete(Object value, List<TupleOID> oids) {
        for (TupleOID oidToDelete : oids) {
            if (value.toString().equals(oidToDelete.toString())) {
                return true;
            }
        }
        return false;
    }



}
