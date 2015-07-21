package speedy.test.utility;

import java.io.File;
import java.net.URL;
import java.text.NumberFormat;
import java.util.Locale;
import speedy.model.database.ITable;

public class UtilityForTests {

    private static Runtime runtime = Runtime.getRuntime();
    public static final String RESOURCES_FOLDER = "/resources/";

    public static String getResourcesFolder(String fileTask) {
        String resourcesFolder = UtilityForTests.getExternalFolder(RESOURCES_FOLDER);
        return resourcesFolder + fileTask;
    }

    public static String getAbsoluteFileName(String fileName) {
        URL url = UtilityForTests.class.getResource(fileName);
        if(url == null) return null;
        return UtilityForTests.class.getResource(fileName).getFile();
    }

    public static String getExternalFolder(String fileName) {
        File buildDir = new File(UtilityForTests.class.getResource("/").getFile()).getParentFile();
        File rootDir = buildDir.getParentFile();
        String miscDir = rootDir.toString() + File.separator + "misc";
        return miscDir + fileName;
    }

    public static long getSize(ITable table) {
        return table.getSize();
    }

    public static String getMemInfo() {
        NumberFormat format = NumberFormat.getInstance(Locale.ENGLISH);
        StringBuilder sb = new StringBuilder();
        long allocatedMemory = runtime.totalMemory();
        sb.append(format.format(allocatedMemory / 1024 / 1024)).append(" MB");
        return sb.toString();

    }

}
