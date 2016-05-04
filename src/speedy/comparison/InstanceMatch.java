package speedy.comparison;

import java.util.List;
import speedy.model.database.IDatabase;
import speedy.utility.SpeedyUtility;

public class InstanceMatch {
    
    private final IDatabase sourceDb;
    private final IDatabase targetDb;
    private TupleMapping tupleMatch;
    private List<TupleWithTable> nonMatchingTuples;

    public InstanceMatch(IDatabase sourceDb, IDatabase targetDb) {
        this.sourceDb = sourceDb;
        this.targetDb = targetDb;
    }

    public IDatabase getSourceDb() {
        return sourceDb;
    }

    public IDatabase getTargetDb() {
        return targetDb;
    }
    
    public boolean hasHomomorphism() {
        return this.tupleMatch != null;
    }

    public TupleMapping getTupleMatch() {
        return tupleMatch;
    }

    public void setTupleMatch(TupleMapping tupleMatch) {
        this.tupleMatch = tupleMatch;
    }

    public List<TupleWithTable> getNonMatchingTuples() {
        return nonMatchingTuples;
    }

    public void setNonMatchingTuples(List<TupleWithTable> nonMatchingTuples) {
        this.nonMatchingTuples = nonMatchingTuples;
    }

    @Override
    public String toString() {
        return "InstanceMatch[\n" 
                + tupleMatch 
                + ((nonMatchingTuples != null && !nonMatchingTuples.isEmpty()) ? 
                        "\nNon matching tuples=" + SpeedyUtility.printCollection(nonMatchingTuples) : "")
                + "\n]";
    }    

}
