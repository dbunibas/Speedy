package speedy.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.exceptions.DAOException;
import speedy.model.database.mainmemory.MainMemoryDB;
import speedy.model.database.mainmemory.datasource.DataSource;
import speedy.persistence.file.DAOFile;
import speedy.persistence.xml.DAOXsd;

public class DAOMainMemoryDatabase {
    
    private DAOXsd daoXSD = new DAOXsd();
    private DAOFile daoFile = new DAOFile();
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
    
    public MainMemoryDB loadCSVDatabase(String instancePath, char separator, Character quoteCharacter) throws DAOException {
        logger.debug("Loading main-memory database. From CSV instances in folder: " + instancePath);
        DataSource dataSource = daoFile.loadSchema(instancePath, separator, quoteCharacter);
        daoFile.loadInstance(dataSource, instancePath, separator, quoteCharacter);
        return new MainMemoryDB(dataSource);
    }
    
   
}
