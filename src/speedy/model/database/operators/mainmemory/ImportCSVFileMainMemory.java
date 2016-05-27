package speedy.model.database.operators.mainmemory;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.SpeedyConstants;
import speedy.exceptions.DAOException;
import speedy.model.database.IValue;
import speedy.model.database.NullValue;
import speedy.model.database.mainmemory.datasource.DataSource;
import speedy.model.database.mainmemory.datasource.INode;
import speedy.model.database.mainmemory.datasource.IntegerOIDGenerator;
import speedy.model.database.mainmemory.datasource.nodes.AttributeNode;
import speedy.model.database.mainmemory.datasource.nodes.LeafNode;
import speedy.model.database.mainmemory.datasource.nodes.SetNode;
import speedy.model.database.mainmemory.datasource.nodes.TupleNode;
import speedy.persistence.PersistenceConstants;
import speedy.persistence.Types;
import speedy.utility.DBMSUtility;

public class ImportCSVFileMainMemory {

    private static final String CSV_EXTENSION = ".csv";
    private static final Logger logger = LoggerFactory.getLogger(ImportCSVFileMainMemory.class);

    public DataSource loadSchema(String instancePath, char separator, Character quoteCharacter) {
        List<File> filesTable = getFileInFolder(instancePath, CSV_EXTENSION);
        Map<File, CSVTable> mapTable = loadTable(filesTable, separator, quoteCharacter);
        INode schemaNode = new TupleNode(PersistenceConstants.DATASOURCE_ROOT_LABEL, IntegerOIDGenerator.getNextOID());
        schemaNode.setRoot(true);
        generateSchema(schemaNode, mapTable);
        DataSource dataSource = new DataSource(PersistenceConstants.TYPE_CSV, schemaNode);
        if (logger.isDebugEnabled()) logger.debug(dataSource.getSchema().toString());
        return dataSource;
    }

    public void loadInstance(DataSource dataSource, String instancePath, char separator, Character quoteCharacter, boolean convertSkolemInHash) {
        List<File> filesTable = getFileInFolder(instancePath, CSV_EXTENSION);
        Map<File, CSVTable> mapTable = loadTable(filesTable, separator, quoteCharacter);
        INode instanceNode = new TupleNode(PersistenceConstants.DATASOURCE_ROOT_LABEL, IntegerOIDGenerator.getNextOID());
        instanceNode.setRoot(true);
        insertData(instanceNode, mapTable, separator, quoteCharacter, convertSkolemInHash);
        dataSource.addInstanceWithCheck(instanceNode);
    }

    private Map<File, CSVTable> loadTable(List<File> filesTable, char separator, Character quoteCharacter) throws DAOException {
        Map<File, CSVTable> mapFileToTable = new HashMap<File, CSVTable>();
        for (File file : filesTable) {
            try {
                CsvMapper mapper = new CsvMapper();
                mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
                CsvSchema schema = CsvSchema.emptySchema().
                        withColumnSeparator(separator).
                        withQuoteChar(quoteCharacter);
                MappingIterator<String[]> it = mapper.readerFor(String[].class).with(schema).readValues(file);
                if (!it.hasNext()) {
                    throw new DAOException("Empty file " + file);
                }
                String[] headers = it.next();
                String tableName = extractTableName(file, CSV_EXTENSION);
                CSVTable table = new CSVTable(tableName);
                for (String attributeName : headers) {
                    table.addAttribute(attributeName);
                }
                mapFileToTable.put(file, table);
            } catch (Exception ex) {
                throw new DAOException(ex.getMessage());
            }
        }
        return mapFileToTable;
    }

    private String extractTableName(File file, String CSV_EXTENSION) {
        return file.getName().replaceAll(CSV_EXTENSION, "");
    }

    private void generateSchema(INode schemaNode, Map<File, CSVTable> mapTable) {
        Collection<CSVTable> csvTables = mapTable.values();
        for (CSVTable csvTable : csvTables) {
            INode setNodeSchema = new SetNode(csvTable.getName());
            schemaNode.addChild(setNodeSchema);
            TupleNode tupleNodeSchema = new TupleNode(csvTable.getName() + "Tuple");
            setNodeSchema.addChild(tupleNodeSchema);
            for (String attribute : csvTable.getAttributes()) {
                tupleNodeSchema.addChild(createAttributeSchema(attribute));
            }
        }
    }

    private AttributeNode createAttributeSchema(String attributeName) {
        AttributeNode attributeNodeInstance = new AttributeNode(attributeName);
        LeafNode leafNodeInstance = new LeafNode(Types.STRING, SpeedyConstants.NULL_VALUE);
        attributeNodeInstance.addChild(leafNodeInstance);
        return attributeNodeInstance;
    }

    private void insertData(INode setNodeDB, Map<File, CSVTable> mapTable, char separator, Character quoteCharacter, boolean convertSkolemInHash) {
        Set<File> fileSet = mapTable.keySet();
        for (File file : fileSet) {
            CSVTable csvTable = mapTable.get(file);
            addTable(setNodeDB, csvTable, file, separator, quoteCharacter, convertSkolemInHash);
        }
    }

    private void addTable(INode setNodeDB, CSVTable csvTable, File file, char separator, Character quoteCharacter, boolean convertSkolemInHash) {
        INode setNodeTable = new SetNode(csvTable.getName(), IntegerOIDGenerator.getNextOID());
        setNodeDB.addChild(setNodeTable);
        try {
            CsvMapper mapper = new CsvMapper();
            mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
            CsvSchema schema = CsvSchema.emptySchema().
                    withColumnSeparator(separator).
                    withQuoteChar(quoteCharacter);
            MappingIterator<String[]> it = mapper.readerFor(String[].class).with(schema).readValues(file);
            if (!it.hasNext()) {
                throw new DAOException("Empty file " + file);
            }
            it.next(); //Skipping header
            insertDataInTable(setNodeTable, csvTable, it, convertSkolemInHash);
        } catch (Exception ex) {
            throw new DAOException("Unable to load csv file " + file.toString() + "\n" + ex.getLocalizedMessage());
        }
    }

    private AttributeNode createAttributeInstance(String attributeName, Object objValue, boolean convertSkolemInHash) {
        AttributeNode attributeNodeInstance = new AttributeNode(attributeName, IntegerOIDGenerator.getNextOID());
        IValue value = DBMSUtility.convertDBMSValue(objValue);
        if (convertSkolemInHash && (value instanceof NullValue) && ((NullValue) value).isLabeledNull()) {
            value = new NullValue(SpeedyConstants.SKOLEM_PREFIX + value.hashCode());
        }
        LeafNode leafNodeInstance = new LeafNode(Types.STRING, value);
        attributeNodeInstance.addChild(leafNodeInstance);
        return attributeNodeInstance;
    }

    private void insertDataInTable(INode setNodeTable, CSVTable csvTable, MappingIterator<String[]> it, boolean convertSkolemInHash) {
        while (it.hasNext()) {
            String[] record = it.next();
            TupleNode tupleNodeInstance = new TupleNode(csvTable.getName() + "Tuple", IntegerOIDGenerator.getNextOID());
            setNodeTable.addChild(tupleNodeInstance);
            for (int i = 0; i < csvTable.getAttributes().size(); i++) {
                String attributeName = csvTable.getAttributes().get(i);
                String value = record[i];
                AttributeNode attributeNode = createAttributeInstance(attributeName, value, convertSkolemInHash);
                tupleNodeInstance.addChild(attributeNode);
            }
        }
    }

    private List<File> getFileInFolder(String folderPath, String extension) {
        File folder = new File(folderPath);
        if (!folder.exists()) {
            throw new DAOException("Folder " + folder + " doesn't exist");
        }
        List<File> files = new ArrayList<File>();
        File[] listFiles = folder.listFiles();
        for (File file : listFiles) {
            if (file.isFile() && (extension == null || file.getName().endsWith(extension))) {
                files.add(file);
            }
        }
        return files;

    }

    private class CSVTable {

        private String name;
        private List<String> attributes = new ArrayList<String>();

        public CSVTable(String name) {
            this.name = name;
        }

        public void addAttribute(String attributeName) {
            this.attributes.add(attributeName);
        }

        public String getName() {
            return name;
        }

        public List<String> getAttributes() {
            return attributes;
        }

    }

}
