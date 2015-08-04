package speedy.model.database.operators.dbms;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import speedy.SpeedyConstants;
import speedy.exceptions.DAOException;
import speedy.model.database.Attribute;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.database.dbms.InitDBConfiguration;
import speedy.persistence.Types;
import speedy.persistence.relational.AccessConfiguration;
import speedy.utility.DBMSUtility;
import speedy.persistence.relational.QueryManager;
import speedy.persistence.xml.DAOXmlUtility;
import speedy.utility.SpeedyUtility;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.jdom.Document;
import org.jdom.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecuteInitDB {

    private static Logger logger = LoggerFactory.getLogger(ExecuteInitDB.class);
    private DAOXmlUtility daoUtility = new DAOXmlUtility();

    public void execute(DBMSDB db) {
        InitDBConfiguration configuration = db.getInitDBConfiguration();
        if (logger.isDebugEnabled()) logger.debug("Initializating DB with configuration " + configuration);
        AccessConfiguration accessConfiguration = db.getAccessConfiguration();
        if (configuration.getInitDBScript() == null && configuration.hasFilesToImport()) {
            configuration.setInitDBScript(createSchemaScript(accessConfiguration.getSchemaName()));
        }
        if (configuration.getInitDBScript() != null) {
            QueryManager.executeScript(configuration.getInitDBScript(), accessConfiguration, false, true, false, false);
        }
        if (configuration.hasFilesToImport()) {
            importXMLFiles(db);
        }
    }

    private void importXMLFiles(DBMSDB db) {
        InitDBConfiguration configuration = db.getInitDBConfiguration();
        Map<String, List<Attribute>> tablesAdded = new HashMap<String, List<Attribute>>();
        for (String tableName : configuration.getTablesToImport()) {
            for (String fileName : configuration.getFilesToImport(tableName)) {
                if (logger.isDebugEnabled()) logger.debug("Importing file " + fileName + " into table " + tableName);
                if (fileName.toUpperCase().endsWith(SpeedyConstants.XML)) {
                    importXMLFile(tableName, fileName, tablesAdded, db);
                } else if (fileName.toUpperCase().endsWith(SpeedyConstants.CSV)) {
                    importCSVFile(tableName, fileName, tablesAdded, db);
                } else {
                    throw new DAOException("Unsupported file: " + fileName);
                }
            }
        }
    }

    ///// XML
    @SuppressWarnings("unchecked")
    private void importXMLFile(String tableName, String xmlFile, Map<String, List<Attribute>> tablesAdded, DBMSDB db) {
        InitDBConfiguration configuration = db.getInitDBConfiguration();
        try {
            Document document = daoUtility.buildDOM(xmlFile);
            Element tableElement = document.getRootElement();
            if (tableElement.getChildren().isEmpty()) {
                throw new DAOException("Unable to import file from empty file " + xmlFile);
            }
            System.out.println("Importing file " + xmlFile + " into table " + tableName + "...");
            if (!tablesAdded.containsKey(tableName)) {
                List<Attribute> attributes = createXMLTable(tableName, tableElement, db.getAccessConfiguration(), configuration.isCreateTablesFromFiles());
                tablesAdded.put(tableName, attributes);
            }
            List<Attribute> attributes = tablesAdded.get(tableName);
            insertXMLTuples(tableName, attributes, tableElement, db.getAccessConfiguration(), xmlFile);
        } catch (DAOException ex) {
            logger.error(ex.getLocalizedMessage());
            ex.printStackTrace();
            String message = "Unable to load XML file " + xmlFile;
            if (ex.getMessage() != null && !ex.getMessage().equals("NULL")) {
                message += "\n" + ex.getMessage();
            }
            throw new DAOException(message);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Attribute> createXMLTable(String tableName, Element tableElement, AccessConfiguration accessConfiguration, boolean createTable) {
        List<Attribute> attributes = new ArrayList<Attribute>();
        StringBuilder sb = new StringBuilder();
        sb.append("create table ").append(DBMSUtility.getSchema(accessConfiguration)).append(tableName).append("(\n");
        sb.append(SpeedyConstants.INDENT).append("oid serial,\n");
        Element firstChild = (Element) tableElement.getChildren().get(0);
        for (Element attributeElement : (List<Element>) firstChild.getChildren()) {
            String attributeName = attributeElement.getName();
            String attributeType = Types.STRING;
            if (attributeElement.getAttribute("type") != null) {
                attributeType = attributeElement.getAttribute("type").getValue();
            }
            Attribute attribute = new Attribute(tableName, attributeName, attributeType);
            attributes.add(attribute);
            sb.append(SpeedyConstants.INDENT).append(attributeName).append(" ").append(DBMSUtility.convertDataSourceTypeToDBType(attributeType)).append(",\n");
        }
        SpeedyUtility.removeChars(",\n".length(), sb);
//        sb.append(") with oids;");
        sb.append(");");
        if (logger.isDebugEnabled()) logger.debug("Executing script " + sb.toString());
        if (createTable) {
            QueryManager.executeScript(sb.toString(), accessConfiguration, false, true, false, false);
        }
        return attributes;
    }

    @SuppressWarnings("unchecked")
    private void insertXMLTuples(String tableName, List<Attribute> attributes, Element tableElement, AccessConfiguration accessConfiguration, String xmlFile) {
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (Element tupleElement : (List<Element>) tableElement.getChildren()) {
            if (count > 0 && count % 10000 == 0) {
                QueryManager.executeScript(sb.toString(), accessConfiguration, false, true, false, false);
                sb = new StringBuilder();
//                if (task.getConfiguration().isPrint()) 
                System.out.println("..." + count + " tuple inserted in table " + tableName);
                if (logger.isDebugEnabled()) logger.debug("..." + count + " tuple inserted in table " + tableName);
            }
            sb.append("insert into ").append(DBMSUtility.getSchema(accessConfiguration)).append(tableName).append("(");
            for (Attribute attribute : attributes) {
                sb.append(attribute.getName()).append(", ");
            }
            SpeedyUtility.removeChars(", ".length(), sb);
            sb.append(") values (");
            for (Attribute attribute : attributes) {
                Element attributeElement = tupleElement.getChild(attribute.getName());
                if (attributeElement == null) {
                    throw new DAOException("Error importing " + xmlFile + ". Attribute " + attribute.getName() + " in table " + tableName + " is missing");
                }
                String value = attributeElement.getText();
                if (notNull(value) && attribute.getType().equals(Types.STRING)) sb.append("'");
                sb.append(cleanValue(value));
                if (notNull(value) && attribute.getType().equals(Types.STRING)) sb.append("'");
                sb.append(", ");
            }
            SpeedyUtility.removeChars(", ".length(), sb);
            sb.append(");\n");
            count++;
        }
        if (sb.toString().isEmpty()) {
            return;
        }
//        if (task.getConfiguration().isPrint()) 
        System.out.println(count + " tuple inserted in table " + tableName);
        if (logger.isDebugEnabled()) logger.debug(count + " tuple inserted in table " + tableName);
        QueryManager.executeScript(sb.toString(), accessConfiguration, false, true, false, false);
    }

    ///// CSV
    private void importCSVFile(String tableName, String csvFile, Map<String, List<Attribute>> tablesAdded, DBMSDB db) {
        InitDBConfiguration configuration = db.getInitDBConfiguration();
        Reader in = null;
        try {
            in = new FileReader(csvFile);
            CSVFormat format = CSVFormat.newFormat(';').withHeader();
            CSVParser parser = format.parse(in);
            List<Attribute> attributes = readCSVAttributes(tableName, parser.getHeaderMap().keySet());
            Iterable<CSVRecord> records = parser.getRecords();
            if (!records.iterator().hasNext()) {
                throw new DAOException("Unable to import file from empty file " + csvFile);
            }
            System.out.println("Importing file " + csvFile + " into table " + tableName + "...");
            if (!tablesAdded.containsKey(tableName)) {
                createCSVTable(tableName, attributes, db.getAccessConfiguration(), configuration.isCreateTablesFromFiles());
                tablesAdded.put(tableName, attributes);
            }
            insertCSVTuples(tableName, attributes, records, db.getAccessConfiguration(), csvFile);
        } catch (Exception ex) {
            logger.error(ex.getLocalizedMessage());
            ex.printStackTrace();
            String message = "Unable to load CSV file " + csvFile;
            if (ex.getMessage() != null && !ex.getMessage().equals("NULL")) {
                message += "\n" + ex.getMessage();
            }
            throw new DAOException(message);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ex) {
                }
            }
        }
    }

    private List<Attribute> readCSVAttributes(String tableName, Set<String> headers) {
        List<Attribute> attributes = new ArrayList<Attribute>();
        for (String attributeName : headers) {
            String attributeType = Types.STRING;
            String integerSuffix = "(" + Types.INTEGER + ")";
            if (attributeName.endsWith(integerSuffix)) {
                attributeType = Types.INTEGER;
                attributeName = attributeName.substring(0, attributeName.length() - integerSuffix.length()).trim();
            }
            String doubleSuffix = "(" + Types.DOUBLE + ")";
            if (attributeName.endsWith(doubleSuffix)) {
                attributeType = Types.DOUBLE;
                attributeName = attributeName.substring(0, attributeName.length() - doubleSuffix.length()).trim();
            }
            String booleanSuffix = "(" + Types.BOOLEAN + ")";
            if (attributeName.endsWith(booleanSuffix)) {
                attributeType = Types.BOOLEAN;
                attributeName = attributeName.substring(0, attributeName.length() - booleanSuffix.length()).trim();
            }
            Attribute attribute = new Attribute(tableName, attributeName, attributeType);
            attributes.add(attribute);
        }
        return attributes;
    }

    private List<Attribute> createCSVTable(String tableName, List<Attribute> attributes, AccessConfiguration accessConfiguration, boolean createTable) {
        StringBuilder sb = new StringBuilder();
        sb.append("create table ").append(DBMSUtility.getSchema(accessConfiguration)).append(tableName).append("(\n");
        sb.append(SpeedyConstants.INDENT).append("oid serial,\n");
        for (Attribute attribute : attributes) {
            String attributeName = attribute.getName();
            String attributeType = attribute.getType();
            sb.append(SpeedyConstants.INDENT).append(attributeName).append(" ").append(DBMSUtility.convertDataSourceTypeToDBType(attributeType)).append(",\n");
        }
        SpeedyUtility.removeChars(",\n".length(), sb);
//        sb.append(") with oids;");
        sb.append(");");
        if (logger.isDebugEnabled()) logger.debug("Executing script " + sb.toString());
        if (createTable) {
            QueryManager.executeScript(sb.toString(), accessConfiguration, false, true, false, false);
        }
        return attributes;
    }

    private void insertCSVTuples(String tableName, List<Attribute> attributes, Iterable<CSVRecord> records, AccessConfiguration accessConfiguration, String csvFile) {
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (CSVRecord record : records) {
            if (count > 0 && count % 10000 == 0) {
                QueryManager.executeScript(sb.toString(), accessConfiguration, false, true, false, false);
                sb = new StringBuilder();
//                if (task.getConfiguration().isPrint()) 
                System.out.println("..." + count + " tuple inserted in table " + tableName);
                if (logger.isDebugEnabled()) logger.debug("..." + count + " tuple inserted in table " + tableName);
            }
            sb.append("insert into ").append(DBMSUtility.getSchema(accessConfiguration)).append(tableName).append("(");
            for (Attribute attribute : attributes) {
                sb.append(attribute.getName()).append(", ");
            }
            SpeedyUtility.removeChars(", ".length(), sb);
            sb.append(") values (");
            for (int i = 0; i < attributes.size(); i++) {
                Attribute attribute = attributes.get(i);
                String value = record.get(i);
                if (value == null) {
                    throw new DAOException("Error importing " + csvFile + ". Attribute " + attribute.getName() + " in table " + tableName + " is missing");
                }
                if (notNull(value) && attribute.getType().equals(Types.STRING)) sb.append("'");
                sb.append(cleanValue(value));
                if (notNull(value) && attribute.getType().equals(Types.STRING)) sb.append("'");
                sb.append(", ");
            }
            SpeedyUtility.removeChars(", ".length(), sb);
            sb.append(");\n");
            count++;
        }
        if (sb.toString().isEmpty()) {
            return;
        }
//        if (task.getConfiguration().isPrint()) 
        System.out.println(count + " tuple inserted in table " + tableName);
        if (logger.isDebugEnabled()) logger.debug(count + " tuple inserted in table " + tableName);
        QueryManager.executeScript(sb.toString(), accessConfiguration, false, true, false, false);
    }

    private String cleanValue(String string) {
        String sqlValue = string;
        sqlValue = sqlValue.replaceAll("'", "''");
        return sqlValue;
    }

    private String createSchemaScript(String schemaName) {
        if (schemaName.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("create schema ").append(schemaName).append(";\n");
        return sb.toString();
    }

    private boolean notNull(String value) {
        return value != null && !value.equalsIgnoreCase("NULL");
    }

}
