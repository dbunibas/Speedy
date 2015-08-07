package speedy.persistence.file;

import speedy.SpeedyConstants;

public class XMLFile implements IImportFile {

    private String fileName;

    public XMLFile(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    public String getType() {
        return SpeedyConstants.XML;
    }

}
