package speedy.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.exceptions.DAOException;
import speedy.model.database.dbms.DBMSDB;
import speedy.persistence.relational.AccessConfiguration;

public class DAODBMSDatabase {

    private static Logger logger = LoggerFactory.getLogger(DAODBMSDatabase.class);

    public DBMSDB loadXMLDatabase(String driver, String uri, String schemaName, String username, String password) throws DAOException {
        AccessConfiguration accessConfiguration = new AccessConfiguration();
        accessConfiguration.setDriver(driver);
        accessConfiguration.setUri(uri);
        accessConfiguration.setSchemaName(schemaName);
        accessConfiguration.setLogin(username);
        accessConfiguration.setPassword(password);
//        Element initDbElement = databaseElement.getChild("init-db");
        DBMSDB database = new DBMSDB(accessConfiguration);
//        if (initDbElement != null) {
//            database.getInitDBConfiguration().setInitDBScript(initDbElement.getValue());
//        }
//        Element importXmlElement = databaseElement.getChild("import-xml");
//        if (importXmlElement != null) {
//            Attribute createTableAttribute = importXmlElement.getAttribute("createTables");
//            if (createTableAttribute != null) {
//                database.getInitDBConfiguration().setCreateTablesFromXML(Boolean.parseBoolean(createTableAttribute.getValue()));
//            }
//            for (Object inputFileObj : importXmlElement.getChildren("input")) {
//                Element inputFileElement = (Element) inputFileObj;
//                String xmlFile = inputFileElement.getText();
//                xmlFile = filePathTransformator.expand(fileScenario, xmlFile);
//                database.getInitDBConfiguration().addXmlFileToImport(xmlFile);
//            }
//        }
        return database;
    }
}
