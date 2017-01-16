package speedy.test;

import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.ComparisonConfiguration;
import speedy.comparison.ComparisonStats;
import speedy.model.database.IDatabase;
import speedy.model.database.mainmemory.MainMemoryDB;
import speedy.persistence.DAOMainMemoryDatabase;
import speedy.utility.test.UtilityForTests;

public class ComparisonUtilityTest {

    private final static Logger logger = LoggerFactory.getLogger(ComparisonUtilityTest.class);
    private final static DAOMainMemoryDatabase dao = new DAOMainMemoryDatabase();

    public static IDatabase loadDatabase(String folderName, String baseFolder) {
        String folder = UtilityForTests.getAbsoluteFileName(baseFolder + File.separator + folderName);
        return loadDatabase(folder);
    }

    public static IDatabase loadDatabase(String absoluteFolder) {
        long start = System.currentTimeMillis();
        boolean convertSkolemInHash = ComparisonConfiguration.isConvertSkolemInHash();
        MainMemoryDB database = dao.loadCSVDatabase(absoluteFolder, ',', null, convertSkolemInHash, true);
        if (logger.isDebugEnabled()) logger.debug(database.printInstances(true));
        ComparisonStats.getInstance().addStat(ComparisonStats.LOAD_INSTANCE_TIME, System.currentTimeMillis() - start);
        return database;
    }
}
