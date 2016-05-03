package speedy.comparison.operators;

import speedy.comparison.InstanceMatch;
import speedy.model.database.IDatabase;

public interface IComputeInstanceSimilarity {

    public InstanceMatch compare(IDatabase leftInstance, IDatabase rightInstance);
}
