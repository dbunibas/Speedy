package speedy.persistence.file;

import speedy.SpeedyConstants;

public class CSVFile implements IImportFile {

    private String fileName;
    private Integer recordsToImport;
    private char separator = ';';

    public CSVFile(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    public String getType() {
        return SpeedyConstants.CSV;
    }

    public Integer getRecordsToImport() {
        return recordsToImport;
    }

    public void setRecordsToImport(Integer recordsToImport) {
        this.recordsToImport = recordsToImport;
    }

    public char getSeparator() {
        return separator;
    }

    public void setSeparator(char separator) {
        this.separator = separator;
    }

}
