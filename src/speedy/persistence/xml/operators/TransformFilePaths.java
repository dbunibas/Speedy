package speedy.persistence.xml.operators;

import speedy.utility.SpeedyUtility;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformFilePaths {

    private static Logger logger = LoggerFactory.getLogger(TransformFilePaths.class);

    public String relativize(String baseFilePath, String relativeFilePath) {
        if (baseFilePath == null || baseFilePath.trim().isEmpty()) {
            throw new IllegalArgumentException("Unable to relativize path from a null base path");
        }
        baseFilePath = SpeedyUtility.generateFolderPath(baseFilePath);
        List<String> basePathSteps = getPathSteps(baseFilePath);
        if (logger.isDebugEnabled()) logger.debug("Base path steps: " + basePathSteps);
        List<String> filePathSteps = getPathSteps(relativeFilePath);
        if (logger.isDebugEnabled()) logger.debug("File path steps: " + filePathSteps);
        String s = findRelativePathList(basePathSteps, filePathSteps);
        if (logger.isDebugEnabled()) logger.debug("Result: " + s);
        return s;
    }

    public String expand(String baseFilePath, String filePath) {
        if (baseFilePath == null || baseFilePath.trim().isEmpty()) {
            throw new IllegalArgumentException("Unable to expand path frin a null base path");
        }
        if (isAbsolutePath(filePath)) {
            if (logger.isDebugEnabled()) logger.debug("File path is absolute. Nothing to expand...");
            return filePath;
        }
        if (logger.isDebugEnabled()) logger.debug("Expanding filePath: " + filePath + " wrt base path " + baseFilePath);
        baseFilePath = SpeedyUtility.generateFolderPath(baseFilePath);
        List<String> basePathSteps = getPathSteps(baseFilePath);
        if (logger.isDebugEnabled()) logger.debug("Base path steps: " + basePathSteps);
        List<String> filePathSteps = getPathSteps(filePath);
        if (logger.isDebugEnabled()) logger.debug("File path steps: " + filePathSteps);
        String s = mergePathLists(basePathSteps, filePathSteps);
        if (logger.isDebugEnabled()) logger.debug("Result: " + s);
        return s;
    }

    private boolean isAbsolutePath(String filePath) {
        return Paths.get(filePath).isAbsolute();
    }

    private List<String> getPathSteps(String filePath) {
        List<String> result = new ArrayList<String>();
        String separators = "/\\";
        StringTokenizer tokenizer = new StringTokenizer(filePath, separators);
        while (tokenizer.hasMoreTokens()) {
            result.add(0, tokenizer.nextToken());
        }
        return result;
    }

    private String findRelativePathList(List basePathSteps, List filePathSteps) {
        int i;
        int j;
        String s = "";
        i = basePathSteps.size() - 1;
        j = filePathSteps.size() - 1;

        // first eliminate common root
        while ((i >= 0) && (j >= 0) && (basePathSteps.get(i).equals(filePathSteps.get(j)))) {
            i--;
            j--;
        }

        // for each remaining level in the base path, add a ..
        for (; i >= 0; i--) {
            s += ".." + File.separator;
        }

        // for each level in the file path, add the path
        for (; j >= 1; j--) {
            s += filePathSteps.get(j) + File.separator;
        }

        // file name
        s += filePathSteps.get(j);
        return s;
    }

    private String mergePathLists(List<String> basePathSteps, List<String> filePathSteps) {
        Collections.reverse(basePathSteps);
        Collections.reverse(filePathSteps);
        List<String> result = new ArrayList<String>(basePathSteps);
        int i = 0;
        while (i < filePathSteps.size() && filePathSteps.get(i).equals("..")) {
            result.remove(result.size() - 1);
            i++;
        }
        for (int j = i; j < filePathSteps.size(); j++) {
            result.add(filePathSteps.get(j));
        }
        StringBuilder resultPath = new StringBuilder();
        for (int k = 0; k < result.size(); k++) {
            resultPath.append(result.get(k));
            if (k != result.size() - 1) {
                resultPath.append(File.separator);
            }
        }
        String resultString = resultPath.toString();
        if (!resultString.startsWith(File.separator)) {
            resultString = File.separator + resultString;
        }
        return resultString;
    }
}
