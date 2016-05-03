package speedy.comparison.operators;

import speedy.comparison.SimilarityResult;
import speedy.model.database.IDatabase;

public interface ICompareInstances {

    public SimilarityResult compare(IDatabase expected, IDatabase generated);
}
