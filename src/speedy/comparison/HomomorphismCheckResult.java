package speedy.comparison;

import java.util.List;
import speedy.model.database.IDatabase;
import speedy.utility.SpeedyUtility;

public class HomomorphismCheckResult {
    
    private final IDatabase sourceDb;
    private final IDatabase targetDb;
    private Homomorphism homomorphism;
    private List<TupleWithTable> nonMatchingTuples;

    public HomomorphismCheckResult(IDatabase sourceDb, IDatabase targetDb) {
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
        return this.homomorphism != null;
    }

    public Homomorphism getHomomorphism() {
        return homomorphism;
    }

    public void setHomomorphism(Homomorphism homomorphism) {
        this.homomorphism = homomorphism;
    }

    public List<TupleWithTable> getNonMatchingTuples() {
        return nonMatchingTuples;
    }

    public void setNonMatchingTuples(List<TupleWithTable> nonMatchingTuples) {
        this.nonMatchingTuples = nonMatchingTuples;
    }

    @Override
    public String toString() {
        return "HomomorphismCheckResult[\n" 
                + "Homomorphism:" + homomorphism 
                + "\nNonMatchingTuples=" + SpeedyUtility.printCollection(nonMatchingTuples)
                + "\n]";
    }    

}
