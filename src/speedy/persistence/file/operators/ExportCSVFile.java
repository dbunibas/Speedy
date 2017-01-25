package speedy.persistence.file.operators;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.SpeedyConstants;
import speedy.exceptions.DAOException;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.Attribute;
import speedy.model.database.AttributeRef;
import speedy.model.database.Cell;
import speedy.model.database.IDatabase;
import speedy.model.database.ITable;
import speedy.model.database.IValue;
import speedy.model.database.Tuple;
import speedy.model.database.operators.dbms.IValueEncoder;
import speedy.model.thread.IBackgroundThread;
import speedy.model.thread.ThreadManager;
import speedy.utility.SpeedyUtility;

public class ExportCSVFile {

    private final static Logger logger = LoggerFactory.getLogger(ExportCSVFile.class);
    private static String SEPARATOR = ",";
    private static String NEW_LINE = "\n";

    public void exportDatabase(IDatabase database, boolean withHeader, boolean withOIDs, String path) {
        exportDatabase(database, null, withHeader, withOIDs, path, 4);
    }

    public void exportDatabase(IDatabase database, IValueEncoder valueEncoder, boolean withHeader, boolean withOIDs, String path, int numberOfThreads) {
        ThreadManager threadManager = new ThreadManager(numberOfThreads);
        for (String tableName : database.getTableNames()) {
            ITable table = database.getTable(tableName);
            String fileName = path + "/" + table.getName() + ".csv";
            File outputFile = new File(fileName);
            outputFile.getParentFile().mkdirs();
            ExportTableThread execThread = new ExportTableThread(table, valueEncoder, withHeader, withOIDs, fileName);
            threadManager.startThread(execThread);
        }
        threadManager.waitForActiveThread();
    }

    private class ExportTableThread implements IBackgroundThread {

        private ITable table;
        private IValueEncoder valueEncoder;
        private boolean withHeader;
        private boolean withOIDs;
        private String path;

        public ExportTableThread(ITable table, IValueEncoder valueEncoder, boolean withHeader, boolean withOIDs, String path) {
            this.table = table;
            this.valueEncoder = valueEncoder;
            this.withHeader = withHeader;
            this.withOIDs = withOIDs;
            this.path = path;
        }

        public void execute() {
            Writer out = null;
            try {
                File outFile = new File(path);
                outFile.getParentFile().mkdirs();
                out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile), "UTF-8"));
                if(withHeader){
                    out.write(writeHeader(table));
                }
                ITupleIterator it = table.getTupleIterator();
                while (it.hasNext()) {
                    Tuple tuple = it.next();
                    out.write(writeTuple(tuple, table));
                }
            } catch (Exception ex) {
                logger.error("Unable to export cell changes to path " + path + "\n\t" + ex.getLocalizedMessage());
                throw new DAOException(ex);
            } finally {
                if (out != null) {
                    try {
                        out.close();
                    } catch (IOException ex) {
                    }
                }
            }
        }

        private String writeHeader(ITable table) {
            StringBuilder sb = new StringBuilder();
            if(withOIDs){
                sb.append(SpeedyConstants.OID);
                sb.append(SEPARATOR);
            }
            for (Attribute attribute : getAttributes(table)) {
                sb.append(attribute.getName());
                sb.append(SEPARATOR);
            }
            SpeedyUtility.removeChars(SEPARATOR.length(), sb);
            sb.append(NEW_LINE);
            return sb.toString();
        }

        private String writeTuple(Tuple tuple, ITable table) {
            StringBuilder sb = new StringBuilder();
            if(withOIDs){
                sb.append(tuple.getOid());
                sb.append(SEPARATOR);
            }
            for (Attribute attribute : getAttributes(table)) {
                Cell cell = tuple.getCell(new AttributeRef(attribute.getTableName(), attribute.getName()));
                IValue value = cell.getValue();
                sb.append(writeValue(value));
                sb.append(SEPARATOR);
            }
            SpeedyUtility.removeChars(SEPARATOR.length(), sb);
            sb.append(NEW_LINE);
            return sb.toString();
        }

        private String writeValue(IValue value) {
            if (value == null) {
                return "";
            }
            String s = value.toString();
            if (valueEncoder != null) {
                if (!SpeedyUtility.isSkolem(s)) {
                    s = valueEncoder.decode(s);
                }
            }
            if (s.contains(SEPARATOR)) {
                logger.warn("Removing csv separator value " + SEPARATOR + " from " + s);
                s = s.replaceAll(SEPARATOR, "");
            }
            return s;
        }

        private List<Attribute> getAttributes(ITable table) {
            List<Attribute> result = new ArrayList<Attribute>();
            for (Attribute attribute : table.getAttributes()) {
                if (attribute.getName().equalsIgnoreCase(SpeedyConstants.OID)) {
                    continue;
                }
                result.add(attribute);
            }
            return result;
        }
    }
}
