package speedy.persistence.file;

import speedy.SpeedyConstants;

public class CSVFile implements IImportFile {

    private final String fileName;
    private Integer recordsToImport;
    private char separator = ';';
    private Character quoteCharacter;
    private boolean randomizeInput;
    private boolean hasHeader = true;

    public CSVFile(String fileName) {
        this.fileName = fileName;
    }

    public CSVFile(String fileName, char separator) {
        this.fileName = fileName;
        this.separator = separator;
    }

    public CSVFile(String fileName, char separator, Character quoteCharacter) {
        this.fileName = fileName;
        this.separator = separator;
        this.quoteCharacter = quoteCharacter;
    }

    public CSVFile(String fileName, char separator, Character quoteCharacter, int recordsToImport) {
        this.fileName = fileName;
        this.separator = separator;
        this.recordsToImport = recordsToImport;
        this.quoteCharacter = quoteCharacter;
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

    public boolean isHasHeader() {
        return hasHeader;
    }

    public void setHasHeader(boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

}
