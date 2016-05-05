package speedy.comparison;

import java.util.List;
import speedy.model.database.IDatabase;
import speedy.utility.SpeedyUtility;

public class InstanceMatch {

    private final IDatabase sourceDb;
    private final IDatabase targetDb;
    private TupleMapping tupleMapping;
    private List<TupleWithTable> nonMatchingTuples;
    private Boolean isomorphism;

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
        return this.tupleMapping != null;
    }

    public TupleMapping getTupleMapping() {
        return tupleMapping;
    }

    public void setTupleMapping(TupleMapping tupleMapping) {
        this.tupleMapping = tupleMapping;
    }

    public List<TupleWithTable> getNonMatchingTuples() {
        return nonMatchingTuples;
    }

    public void setNonMatchingTuples(List<TupleWithTable> nonMatchingTuples) {
        this.nonMatchingTuples = nonMatchingTuples;
    }

    public Boolean isIsomorphism() {
        return isomorphism;
    }

    public void setIsomorphism(boolean isomorphism) {
        this.isomorphism = isomorphism;
    }

    @Override
    public String toString() {
        return (tupleMapping != null ? tupleMapping : "")
                + (isomorphism != null && isomorphism ? "(isomorphism)\n" : "")
                + ((nonMatchingTuples != null && !nonMatchingTuples.isEmpty())
                        ? "Non matching tuples=" + SpeedyUtility.printCollection(nonMatchingTuples) : "");
    }

}
