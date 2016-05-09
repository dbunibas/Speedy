package speedy.comparison.operators;

import speedy.comparison.InstanceMatchTask;
import speedy.model.database.IDatabase;

public interface IComputeInstanceSimilarity {

    public InstanceMatchTask compare(IDatabase leftInstance, IDatabase rightInstance);
}
