package speedy.persistence.file;

import speedy.SpeedyConstants;

public class CSVFile implements IImportFile {

    private String fileName;
    private Integer recordsToImport;
    private char separator = ';';
    private Character quoteCharacter;
    private boolean randomizeInput;

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

    public Character getQuoteCharacter() {
        return quoteCharacter;
    }

    public void setQuoteCharacter(Character quoteCharacter) {
        this.quoteCharacter = quoteCharacter;
    }

    public boolean isRandomizeInput() {
        return randomizeInput;
    }

    public void setRandomizeInput(boolean randomizeInput) {
        this.randomizeInput = randomizeInput;
    }

}
