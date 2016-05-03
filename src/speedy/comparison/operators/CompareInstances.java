package speedy.comparison.operators;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.SimilarityResult;
import speedy.comparison.TableSimilarity;
import speedy.comparison.TupleMatch;
import speedy.model.database.IDatabase;
import speedy.model.database.ITable;
import speedy.model.database.TupleOID;

public class CompareInstances implements ICompareInstances {

    private final static Logger logger = LoggerFactory.getLogger(CompareInstances.class);

    public SimilarityResult compare(IDatabase expected, IDatabase generated){
        SimilarityResult result = new SimilarityResult();
        for (String tableName : expected.getTableNames()) {
            ITable expectedTable = expected.getTable(tableName);
            ITable generatedTable = generated.getTable(tableName);
            compareTable(expectedTable, generatedTable, result);
        }
        return result;
    }

    private void compareTable(ITable expectedTable, ITable generatedTable, SimilarityResult result) {
        List<TupleMatch> tupleMatches = new ArrayList<TupleMatch>();
        Set<TupleOID> matchedExpected = new HashSet<TupleOID>();
        Set<TupleOID> matchedGenerated = new HashSet<TupleOID>();
        findExactMatches(expectedTable, generatedTable, tupleMatches, matchedExpected, matchedGenerated);
        TableSimilarity similarity = computeSimilarity(tupleMatches, expectedTable, generatedTable);
        if (logger.isDebugEnabled()) logger.debug("Similarity for table " + expectedTable.getName() + ": " + similarity);
        result.setTableSimilarity(expectedTable.getName(), similarity);
    }

    private void findExactMatches(ITable expectedTable, ITable generatedTable, List<TupleMatch> tupleMatches, Set<TupleOID> matchedExpected, Set<TupleOID> matchedGenerated) {
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
