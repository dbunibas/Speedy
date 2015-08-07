package speedy.model.database.dbms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import speedy.persistence.file.IImportFile;

public class InitDBConfiguration {

    private String initDBScript;
    private Map<String, List<IImportFile>> filesToImport = new HashMap<String, List<IImportFile>>();
    private boolean createTablesFromFiles = true;

    public String getInitDBScript() {
        return initDBScript;
    }

    public void setInitDBScript(String initDBScript) {
        this.initDBScript = initDBScript;
    }

    public void addFileToImportForTable(String tableName, IImportFile fileToImport) {
        List<IImportFile> files = filesToImport.get(tableName);
        if (files == null) {
            files = new ArrayList<IImportFile>();
            this.filesToImport.put(tableName, files);
        }
        files.add(fileToImport);
    }

    public boolean hasFilesToImport() {
        return !filesToImport.isEmpty();
    }

    public List<IImportFile> getFilesToImport(String tableName) {
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
