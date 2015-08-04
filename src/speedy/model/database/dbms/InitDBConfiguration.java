package speedy.model.database.dbms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InitDBConfiguration {

    private String initDBScript;
    private Map<String, List<String>> filesToImport = new HashMap<String, List<String>>();
    private boolean createTablesFromFiles = true;

    public String getInitDBScript() {
        return initDBScript;
    }

    public void setInitDBScript(String initDBScript) {
        this.initDBScript = initDBScript;
    }

    public void addFileToImportForTable(String tableName, String fileName) {
        List<String> files = filesToImport.get(tableName);
        if (files == null) {
            files = new ArrayList<String>();
            this.filesToImport.put(tableName, files);
        }
        files.add(fileName);
    }

    public boolean hasFilesToImport() {
        return !filesToImport.isEmpty();
    }

    public List<String> getFilesToImport(String tableName) {
        return filesToImport.get(tableName);
    }

    public Set<String> getTablesToImport() {
        return filesToImport.keySet();
    }

    public boolean isCreateTablesFromFiles() {
        return createTablesFromFiles;
    }

    public void setCreateTablesFromFiles(boolean createTablesFromFiles) {
        this.createTablesFromFiles = createTablesFromFiles;
    }

    public boolean isEmpty() {
        return initDBScript == null && filesToImport.isEmpty();
    }

    @Override
    public String toString() {
        return "InitDBConfiguration{" + "initDBScript=" + initDBScript + ", filesToImport=" + filesToImport + ", createTablesFromXML=" + createTablesFromFiles + '}';
    }

}
