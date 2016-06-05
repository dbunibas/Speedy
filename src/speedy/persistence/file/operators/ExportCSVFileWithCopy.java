package speedy.persistence.file.operators;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.CopyOut;
import org.postgresql.copy.PGCopyOutputStream;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.SpeedyConstants;
import speedy.exceptions.DAOException;
import speedy.model.database.Attribute;
import speedy.model.database.IDatabase;
import speedy.model.database.ITable;
import speedy.model.database.dbms.DBMSTable;
import speedy.model.database.dbms.DBMSVirtualTable;
import speedy.model.database.operators.dbms.IValueEncoder;
import speedy.model.thread.IBackgroundThread;
import speedy.model.thread.ThreadManager;
import speedy.persistence.relational.AccessConfiguration;
import speedy.persistence.relational.QueryManager;
import speedy.utility.DBMSUtility;
import speedy.utility.SpeedyUtility;

public class ExportCSVFileWithCopy {

    private final static Logger logger = LoggerFactory.getLogger(ExportCSVFileWithCopy.class);

    public void exportDatabase(IDatabase database, IValueEncoder valueEncoder, String path, int numberOfThreads) {
        numberOfThreads = Math.min(numberOfThreads, 5);
        ThreadManager threadManager = new ThreadManager(numberOfThreads);
        for (String tableName : database.getTableNames()) {
            ITable table = database.getTable(tableName);
            String fileName = path + "/" + table.getName() + ".csv";
            File outputFile = new File(fileName);
            outputFile.getParentFile().mkdirs();
            ExportTableThread execThread = new ExportTableThread(table, valueEncoder, fileName);
            threadManager.startThread(execThread);
        }
        threadManager.waitForActiveThread();
    }

    class ExportTableThread implements IBackgroundThread {

        private ITable table;
        private IValueEncoder valueEncoder;
        private String fileName;

        public ExportTableThread(ITable table, IValueEncoder valueEncoder, String fileName) {
            this.table = table;
            this.valueEncoder = valueEncoder;
            this.fileName = fileName;
        }

        public void execute() {
            AccessConfiguration ac = getAccessConfiguration(table);
            if (valueEncoder == null) {
                String script = getCopyToScript(table, fileName, ac, true);
                QueryManager.executeScript(script, ac, true, true, true, false);
            } else {
                String script = getCopyToScript(table, fileName, ac, false);
                executeCopyWithEncoding(script, fileName, valueEncoder, ac);
            }
        }

        private String getCopyToScript(ITable table, String file, AccessConfiguration accessConfiguration, boolean useFile) {
            StringBuilder script = new StringBuilder();
            String tableName = getTableName(table);
            script.append("COPY (SELECT ");
            script.append("DISTINCT ");
            for (Attribute attribute : getAttributes(table)) {
                script.append(attribute.getName()).append(", ");
            }
            SpeedyUtility.removeChars(", ".length(), script);
            script.append(" FROM ");
            script.append(DBMSUtility.getSchemaNameAndDot(accessConfiguration)).append(tableName).append(" ");
            script.append(") ");
            if (useFile) {
                script.append("TO '").append(file).append("' ");
            } else {
                script.append("TO STDOUT ");
            }
            script.append(" (");
            script.append("FORMAT CSV, HEADER true");
            script.append(", ");
            script.append("QUOTE '\"'");
            script.append(", ");
            script.append("DELIMITER ',')");
            script.append(";\n");
            if (logger.isDebugEnabled()) logger.debug("--- Import file script:\n" + script.toString());
            return script.toString();
        }

        private Iterable<Attribute> getAttributes(ITable table) {
            List<Attribute> result = new ArrayList<Attribute>();
            for (Attribute attribute : table.getAttributes()) {
                if (attribute.getName().equals(SpeedyConstants.OID)) {
                    continue;
                }
                result.add(attribute);
            }
            return result;
        }

        private void executeCopyWithEncoding(String script, String file, IValueEncoder valueEncoder, AccessConfiguration ac) {
            Connection con = null;
            PrintWriter writer = null;
            try {
                File parentDir = new File(file).getParentFile();
                parentDir.mkdirs();
                writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), Charset.forName("UTF-8")));
                con = DriverManager.getConnection(ac.getUri(), ac.getLogin(), ac.getPassword());
                CopyManager copyManager = new CopyManager((BaseConnection) con);
                CopyOut copyOut = copyManager.copyOut(script);
                copyStream(copyOut, valueEncoder, writer);
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new DAOException(ex);
            } finally {
                if (writer != null) writer.close();
                if (con != null) {
                    try {
                        con.close();
                    } catch (SQLException ex) {
                    }
                }
            }
        }

        private void copyStream(CopyOut copyOut, IValueEncoder valueEncoder, PrintWriter writer) throws SQLException {
            byte[] readFromCopy = copyOut.readFromCopy();
            if (readFromCopy != null) {//Header
                writer.write(new String(readFromCopy));
                readFromCopy = copyOut.readFromCopy();
            }
            while (readFromCopy != null) {
                String line = new String(readFromCopy);
                String[] tokens = line.split(",");
                for (int i = 0; i < tokens.length; i++) {
                    String value = tokens[i].trim();
                    if (!SpeedyUtility.isSkolem(tokens[i])) {
                        value = valueEncoder.decode(value);
                    }
                    writer.append(value);
                    if (i != tokens.length - 1) {
                        writer.append(",");
                    }
                }
                writer.append("\n");
                readFromCopy = copyOut.readFromCopy();
            }
        }

        private AccessConfiguration getAccessConfiguration(ITable table) {
            if (table instanceof DBMSTable) {
                return ((DBMSTable) table).getAccessConfiguration();
            }
            if (table instanceof DBMSVirtualTable) {
                return ((DBMSVirtualTable) table).getAccessConfiguration();
            }
            throw new IllegalArgumentException("Unable to export non-dbms table using copy command");
        }

        private String getTableName(ITable table) {
            if (table instanceof DBMSTable) {
                return ((DBMSTable) table).getName();
            }
            if (table instanceof DBMSVirtualTable) {
                return ((DBMSVirtualTable) table).getVirtualName();
            }
            throw new IllegalArgumentException("Unable to export non-dbms table using copy command");
        }
    }

}
