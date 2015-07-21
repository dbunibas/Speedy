package speedy.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.exceptions.DAOException;
import speedy.model.database.IDatabase;
import speedy.model.database.mainmemory.MainMemoryDB;
import speedy.model.database.mainmemory.datasource.DataSource;
import speedy.persistence.xml.DAOXsd;

public class DAOMainMemoryDatabase {

    private DAOXsd daoXSD = new DAOXsd();
    private static Logger logger = LoggerFactory.getLogger(DAOMainMemoryDatabase.class);

    public MainMemoryDB loadXMLDatabase(String schemaFile, String instanceFile) throws DAOException {
        logger.debug("Loading main-memory database. Schema " + schemaFile + ". Instance " + instanceFile);
        DataSource dataSource = daoXSD.loadSchema(schemaFile);
        if (instanceFile != null) {
            if (logger.isDebugEnabled()) logger.debug("Loading instance");
            daoXSD.loadInstance(dataSource, instanceFile);
        } else {
            PersistenceUtility.createEmptyTables(dataSource);
        }
        return new MainMemoryDB(dataSource);
    }
}
