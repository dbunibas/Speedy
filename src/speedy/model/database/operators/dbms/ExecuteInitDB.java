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
import speedy.persistence.relational.QueryManager;
import speedy.persistence.xml.DAOXmlUtility;
import java.util.ArrayList;
import java.util.Collections;
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
import speedy.OperatorFactory;
import speedy.model.algebra.operators.IBatchInsert;
import speedy.model.algebra.operators.ICreateTable;
import speedy.model.database.AttributeRef;
import speedy.model.database.Cell;
import speedy.model.database.ConstantValue;
import speedy.model.database.IDatabase;
import speedy.model.database.IValue;
import speedy.model.database.NullValue;
import speedy.model.database.Tuple;
import speedy.model.database.TupleOID;
import speedy.model.database.mainmemory.datasource.IntegerOIDGenerator;
import speedy.persistence.file.CSVFile;
import speedy.persistence.file.IImportFile;
import speedy.persistence.file.XMLFile;

public class ExecuteInitDB {

    private static Logger logger = LoggerFactory.getLogger(ExecuteInitDB.class);
    private DAOXmlUtility daoUtility = new DAOXmlUtility();
    private ICreateTable tableCreator;
    private IBatchInsert batchInsertOperator;

    public void execute(DBMSDB db) {
        initOperators(db);
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
            for (IImportFile fileToImport : configuration.getFilesToImport(tableName)) {
                if (logger.isDebugEnabled()) logger.debug("Importing file " + fileToImport.getFileName() + " into table " + tableName);
                if (fileToImport.getType().equals(SpeedyConstants.XML)) {
                    importXMLFile(tableName, (XMLFile) fileToImport, tablesAdded, db);
                } else if (fileToImport.getType().equals(SpeedyConstants.CSV)) {
                    importCSVFile(tableName, (CSVFile) fileToImport, tablesAdded, db);
                } else {
                    throw new DAOException("Unsupported file: " + fileToImport.getType());
                }
            }
        }
    }

    ///// XML
    @SuppressWarnings("unchecked")
    private void importXMLFile(String tableName, XMLFile fileToImport, Map<String, List<Attribute>> tablesAdded, DBMSDB db) {
        String xmlFile = fileToImport.getFileName();
        InitDBConfiguration configuration = db.getInitDBConfiguration();
        try {
            Document document = daoUtility.buildDOM(xmlFile);
            Element tableElement = document.getRootElement();
            if (tableElement.getChildren().isEmpty()) {
                throw new DAOException("Unable to import file from empty file " + xmlFile);
            }
            System.out.println("Importing file " + xmlFile + " into table " + tableName + "...");
            if (!tablesAdded.containsKey(tableName)) {
                List<Attribute> attributes = createXMLTable(tableName, tableElement, db, configuration.isCreateTablesFromFiles());
                tablesAdded.put(tableName, attributes);
            }
            List<Attribute> attributes = tablesAdded.get(tableName);
            insertXMLTuples(tableName, attributes, tableElement, db, xmlFile);
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
    private List<Attribute> createXMLTable(String tableName, Element tableElement, DBMSDB database, boolean createTable) {
        List<Attribute> attributes = new ArrayList<Attribute>();
        Element firstChild = (Element) tableElement.getChildren().get(0);
        for (Element attributeElement : (List<Element>) firstChild.getChildren()) {
            String attributeName = attributeElement.getName();
            String attributeType = Types.STRING;
            if (attributeElement.getAttribute("type") != null) {
                attributeType = attributeElement.getAttribute("type").getValue();
            }
            Attribute attribute = new Attribute(tableName, attributeName, attributeType);
            attributes.add(attribute);
        }
        if (createTable) {
            tableCreator.createTable(tableName, attributes, database);
        }
        return attributes;
    }

    @SuppressWarnings("unchecked")
    private void insertXMLTuples(String tableName, List<Attribute> attributes, Element tableElement, IDatabase target, String xmlFile) {
        for (Element tupleElement : (List<Element>) tableElement.getChildren()) {
            TupleOID tupleOID = new TupleOID(IntegerOIDGenerator.getNextOID());
            Tuple tuple = new Tuple(tupleOID);
            for (Attribute attribute : attributes) {
                Element attributeElement = tupleElement.getChild(attribute.getName());
                if (attributeElement == null) {
                    throw new DAOException("Error importing " + xmlFile + ". Attribute " + attribute.getName() + " in table " + tableName + " is missing");
                }
                String stringValue = attributeElement.getText();
                AttributeRef attributeRef = new AttributeRef(attribute.getTableName(), attribute.getName());
                IValue value;
                if (notNull(stringValue)) {
                    value = new ConstantValue(stringValue);
                } else {
                    value = new NullValue(SpeedyConstants.NULL);
                }
                Cell cell = new Cell(tupleOID, attributeRef, value);
                tuple.addCell(cell);
            }
            batchInsertOperator.insert(target.getTable(tableName), tuple, target);
        }
        batchInsertOperator.flush(target);
    }

    ///// CSV
    private void importCSVFile(String tableName, CSVFile fileToImport, Map<String, List<Attribute>> tablesAdded, DBMSDB database) {
        String csvFile = fileToImport.getFileName();
        InitDBConfiguration configuration = database.getInitDBConfiguration();
        Reader in = null;
        try {
            in = new FileReader(csvFile);
            CSVFormat format = CSVFormat.newFormat(fileToImport.getSeparator())
                    .withQuote(fileToImport.getQuoteCharacter())
                    .withHeader();
            CSVParser parser = format.parse(in);
            List<Attribute> attributes = readCSVAttributes(tableName, parser.getHeaderMap().keySet());
            Iterable<CSVRecord> records = parser.getRecords();
            if (!records.iterator().hasNext()) {
                throw new DAOException("Unable to import file from empty file " + csvFile);
            }
            System.out.println("Importing file " + csvFile + " into table " + tableName + "...");
            if (!tablesAdded.containsKey(tableName)) {
                tablesAdded.put(tableName, attributes);
                if (configuration.isCreateTablesFromFiles()) {
                    tableCreator.createTable(tableName, attributes, database);
                }
            }
            insertCSVTuples(tableName, attributes, records, database, csvFile, fileToImport.getRecordsToImport(), fileToImport.isRandomizeInput());
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
            String dateSuffix = "(" + Types.DATE + ")";
            if (attributeName.endsWith(dateSuffix)) {
                attributeType = Types.DATE;
                attributeName = attributeName.substring(0, attributeName.length() - dateSuffix.length()).trim();
            }
            Attribute attribute = new Attribute(tableName, attributeName, attributeType);
            attributes.add(attribute);
        }
        return attributes;
    }

    private void insertCSVTuples(String tableName, List<Attribute> attributes, Iterable<CSVRecord> records, DBMSDB target, String csvFile, Integer recordsToImport, boolean randomizeInput) {
        int importedRecords = 0;
        if (randomizeInput) {
            records = randomizeCSVRecords(records, recordsToImport);
        }
        for (CSVRecord record : records) {
            TupleOID tupleOID = new TupleOID(IntegerOIDGenerator.getNextOID());
            Tuple tuple = new Tuple(tupleOID);
            for (int i = 0; i < attributes.size(); i++) {
                Attribute attribute = attributes.get(i);
                String stringValue = record.get(i);
                if (stringValue == null) {
                    throw new DAOException("Error importing " + csvFile + ". Attribute " + attribute.getName() + " in table " + tableName + " is missing");
                }
                AttributeRef attributeRef = new AttributeRef(attribute.getTableName(), attribute.getName());
                IValue value;
                if (notNull(stringValue)) {
                    value = new ConstantValue(stringValue);
                } else {
                    value = new NullValue(SpeedyConstants.NULL);
                }
                Cell cell = new Cell(tupleOID, attributeRef, value);
                tuple.addCell(cell);
            }
            batchInsertOperator.insert(target.getTable(tableName), tuple, target);
            importedRecords++;
            if (recordsToImport != null && importedRecords >= recordsToImport) {
                break;
            }
        }
        batchInsertOperator.flush(target);
    }

    private Iterable<CSVRecord> randomizeCSVRecords(Iterable<CSVRecord> records, Integer recordsToImport) {
        List<CSVRecord> result = new ArrayList<CSVRecord>();
        int importedRecords = 0;
        for (CSVRecord record : records) {
            result.add(record);
            if (recordsToImport != null && importedRecords >= recordsToImport) {
                break;
            }
        }
        Collections.shuffle(result);
        return result;
    }

//    private String cleanValue(String string) {
//        String sqlValue = string;
//        sqlValue = sqlValue.replaceAll("'", "''");
//        return sqlValue;
//    }
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

    private void initOperators(DBMSDB database) {
        this.tableCreator = OperatorFactory.getInstance().getTableCreator(database);
        this.batchInsertOperator = OperatorFactory.getInstance().getSingletonBatchInsertOperator(database);
    }

}
