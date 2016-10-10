package speedy.persistence.file.operators;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.exceptions.DAOException;
import speedy.model.algebra.operators.ICreateTable;
import speedy.model.database.Attribute;
import speedy.model.database.dbms.DBMSDB;
import speedy.model.database.dbms.InitDBConfiguration;
import speedy.model.database.operators.dbms.IValueEncoder;
import speedy.persistence.Types;
import speedy.persistence.file.CSVFile;
import speedy.persistence.relational.AccessConfiguration;
import speedy.persistence.relational.QueryManager;
import speedy.utility.DBMSUtility;
import speedy.utility.SpeedyUtility;

public class ImportCSVFileWithCopy {

    private final static Logger logger = LoggerFactory.getLogger(ImportCSVFileWithCopy.class);
    private ICreateTable tableCreator;
    private IValueEncoder valueEncoder;

    public ImportCSVFileWithCopy(ICreateTable tableCreator, IValueEncoder valueEncoder) {
        this.tableCreator = tableCreator;
        this.valueEncoder = valueEncoder;
    }

    public void importCSVFile(String tableName, CSVFile fileToImport, Map<String, List<Attribute>> tablesAdded, DBMSDB database) {
        if (logger.isDebugEnabled()) logger.debug("Importing .csv file using copy...");
        String csvFile = fileToImport.getFileName();
        InitDBConfiguration configuration = database.getInitDBConfiguration();
        if (fileToImport.isRandomizeInput()) {
            throw new DAOException("Randomize input is not available using copy statements");
        }
        if (fileToImport.getRecordsToImport() != null) {
            throw new DAOException("Record to import is not available using copy statements");
        }
        Reader in = null;
        try {
            if (logger.isDebugEnabled()) logger.debug("Beginning of try block...");
            List<Attribute> attributes;
            CsvMapper mapper = new CsvMapper();
            mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
            CsvSchema schema = CsvSchema.emptySchema().withColumnSeparator(fileToImport.getSeparator());
            if (fileToImport.getQuoteCharacter() != null) {
                schema = schema.withQuoteChar(fileToImport.getQuoteCharacter());
            }
            MappingIterator<String[]> it = mapper.readerFor(String[].class).with(schema).readValues(new File(csvFile));
            if (!it.hasNext()) {
                throw new DAOException("Empty file " + csvFile);
            }
            if (fileToImport.isHasHeader()) {
                String[] headers = it.next();
                attributes = readCSVAttributes(tableName, headers);
                if (!tablesAdded.containsKey(tableName)) {
                    tablesAdded.put(tableName, attributes);
                    if (configuration.isCreateTablesFromFiles()) {
                        tableCreator.createTable(tableName, attributes, database);
                    }
                }
            } else {
                database.loadTables();
                attributes = SpeedyUtility.extractAttributesFromDB(tableName, database);
            }
            if (valueEncoder != null) {
//                encodeCSVFile(fileToImport, it);
                encodeAndInsertCSVTuples(tableName, attributes, database, it, fileToImport);
            } else {
                insertCSVTuples(tableName, attributes, database, fileToImport);
            }
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

    private List<Attribute> readCSVAttributes(String tableName, String[] headers) {
        List<Attribute> attributes = new ArrayList<Attribute>();
        for (String attributeName : headers) {
            String attributeType = Types.STRING;
            String integerSuffix = "(" + Types.INTEGER + ")";
            if (attributeName.endsWith(integerSuffix)) {
                attributeType = Types.INTEGER;
                attributeName = attributeName.substring(0, attributeName.length() - integerSuffix.length()).trim();
            }
            String doubleSuffix = "(" + Types.REAL + ")";
            if (attributeName.endsWith(doubleSuffix)) {
                attributeType = Types.REAL;
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
            Attribute attribute = new Attribute(tableName.trim(), attributeName.trim(), attributeType);
            attributes.add(attribute);
        }
        return attributes;
    }

    private void insertCSVTuples(String tableName, List<Attribute> attributes, DBMSDB database, CSVFile csvFile) {
        if (logger.isDebugEnabled()) logger.debug("Starting to insert csv tuples...");
        AccessConfiguration accessConfiguration = database.getAccessConfiguration();
        StringBuilder script = new StringBuilder();
        script.append("COPY ").append(DBMSUtility.getSchemaNameAndDot(accessConfiguration)).append(tableName).append(" (");
        for (Attribute attribute : attributes) {
            script.append(attribute.getName()).append(", ");
        }
        SpeedyUtility.removeChars(", ".length(), script);
        script.append(") FROM '").append(csvFile.getFileName()).append("' (");
        script.append("FORMAT CSV, HEADER ").append(csvFile.isHasHeader() ? "true" : "false");
        script.append(", ");
        if (csvFile.getQuoteCharacter() != null) {
            script.append("QUOTE '").append(csvFile.getQuoteCharacter()).append("'");
            script.append(", ");
        }
        script.append("DELIMITER '").append(csvFile.getSeparator()).append("'");
        script.append(" );\n");
//        script.append("ANALYZE ").append(DBMSUtility.getSchemaNameAndDot(accessConfiguration)).append(tableName).append(";");
        if (logger.isDebugEnabled()) logger.debug("--- Import file script:\n" + script.toString());
        QueryManager.executeScript(script.toString(), accessConfiguration, true, true, true, false);
    }

    private void encodeAndInsertCSVTuples(String tableName, List<Attribute> attributes, DBMSDB database, MappingIterator<String[]> it, CSVFile csvFile) {
        Connection con = null;
        try {
            AccessConfiguration ac = database.getAccessConfiguration();
            con = DriverManager.getConnection(ac.getUri(), ac.getLogin(), ac.getPassword());
            CopyManager copyManager = new CopyManager((BaseConnection) con);
            StringBuilder copyScript = new StringBuilder();
            copyScript.append("COPY ").append(DBMSUtility.getSchemaNameAndDot(ac)).append(tableName).append(" (");
            for (Attribute attribute : attributes) {
                copyScript.append(attribute.getName()).append(", ");
            }
            SpeedyUtility.removeChars(", ".length(), copyScript);
            copyScript.append(") FROM STDIN (");
            copyScript.append("FORMAT CSV, HEADER ").append(csvFile.isHasHeader() ? "true" : "false");
            copyScript.append(", ");
            if (csvFile.getQuoteCharacter() != null) {
                copyScript.append("QUOTE '").append(csvFile.getQuoteCharacter()).append("'");
                copyScript.append(", ");
            }
            copyScript.append("DELIMITER '").append(csvFile.getSeparator()).append("'");
            copyScript.append(" );\n");
            CopyIn copyIn = copyManager.copyIn(copyScript.toString());
            copyStream(copyIn, it, csvFile.getSeparator());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new DAOException(ex);
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException ex) {
                }
            }
        }
    }

    private void copyStream(CopyIn copyIn, MappingIterator<String[]> it, char separator) throws SQLException {
        while (it.hasNext()) {
            String[] record = it.next();
            StringBuilder row = new StringBuilder();
            for (String value : record) {
                row.append(valueEncoder.encode(value)).append(separator);
            }
            SpeedyUtility.removeChars(1, row);
            row.append("\n");
            byte[] bytes = row.toString().getBytes();
            copyIn.writeToCopy(bytes, 0, bytes.length);
        }
        copyIn.endCopy();
    }
}
