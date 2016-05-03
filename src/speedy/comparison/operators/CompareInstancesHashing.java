package speedy.comparison.operators;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.InstanceMatch;
import speedy.comparison.SimilarityResult;
import speedy.comparison.TableSimilarity;
import speedy.comparison.TupleMatch;
import speedy.model.database.IDatabase;
import speedy.model.database.ITable;
import speedy.model.database.TupleOID;

public class CompareInstancesHashing implements IComputeInstanceSimilarity {

    private final static Logger logger = LoggerFactory.getLogger(CompareInstancesHashing.class);

    public InstanceMatch compare(IDatabase leftInstance, IDatabase rightInstance) {
        InstanceMatch result = new InstanceMatch(leftInstance, rightInstance);
//        for (String tableName : leftInstance.getTableNames()) {
//            ITable leftTable = leftInstance.getTable(tableName);
//            ITable rightTable = rightInstance.getTable(tableName);
//            compareTable(leftTable, rightTable, result);
//        }
        return result;
    }

    private void compareTable(ITable leftTable, ITable rightTable, SimilarityResult result) {
        List<TupleMatch> tupleMatches = new ArrayList<TupleMatch>();
        Set<TupleOID> matchedExpected = new HashSet<TupleOID>();
        Set<TupleOID> matchedGenerated = new HashSet<TupleOID>();
        findExactMatches(leftTable, rightTable, tupleMatches, matchedExpected, matchedGenerated);
        TableSimilarity similarity = computeSimilarity(tupleMatches, leftTable, rightTable);
        if (logger.isDebugEnabled()) logger.debug("Similarity for table " + leftTable.getName() + ": " + similarity);
        result.setTableSimilarity(leftTable.getName(), similarity);
    }

    private void findExactMatches(ITable leftTable, ITable rightTable, List<TupleMatch> tupleMatches, Set<TupleOID> matchedExpected, Set<TupleOID> matchedGenerated) {
    }

    private TableSimilarity computeSimilarity(List<TupleMatch> tupleMatches, ITable expectedTable, ITable generatedTable) {
        double totalSimilarity = 0.0;
        for (TupleMatch match : tupleMatches) {
            totalSimilarity += match.getSimilarity();
        }
        double precision = totalSimilarity / (double) generatedTable.getSize();
        double recall = totalSimilarity / (double) expectedTable.getSize();
        return new TableSimilarity(totalSimilarity, precision, recall);
    }

}
