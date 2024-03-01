package speedy.model.database.operators;

import speedy.model.database.IDatabase;


public interface IAnalyzeDatabase {
    
    public void analyze(IDatabase database, int maxNumberOfThreads);

}
