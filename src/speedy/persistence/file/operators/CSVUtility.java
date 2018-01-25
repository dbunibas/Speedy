package speedy.persistence.file.operators;

import java.util.ArrayList;
import java.util.List;
import speedy.model.database.Attribute;
import speedy.persistence.Types;

public class CSVUtility {

    public static List<Attribute> readCSVAttributes(String tableName, String[] headers) {
        List<Attribute> attributes = new ArrayList<Attribute>();
        for (String attributeName : headers) {
            String attributeType = Types.STRING;
            String integerSuffix = "(" + Types.INTEGER + ")";
            if (attributeName.toLowerCase().endsWith(integerSuffix.toLowerCase())) {
                attributeType = Types.INTEGER;
                attributeName = attributeName.substring(0, attributeName.length() - integerSuffix.length()).trim();
            }
            String doubleSuffix = "(" + Types.REAL + ")";
            if (attributeName.toLowerCase().endsWith(doubleSuffix.toLowerCase())) {
                attributeType = Types.REAL;
                attributeName = attributeName.substring(0, attributeName.length() - doubleSuffix.length()).trim();
            }
            String longSuffix = "(" + Types.LONG + ")";
            if (attributeName.toLowerCase().endsWith(longSuffix.toLowerCase())) {
                attributeType = Types.LONG;
                attributeName = attributeName.substring(0, attributeName.length() - longSuffix.length()).trim();
            }
            String booleanSuffix = "(" + Types.BOOLEAN + ")";
            if (attributeName.toLowerCase().endsWith(booleanSuffix.toLowerCase())) {
                attributeType = Types.BOOLEAN;
                attributeName = attributeName.substring(0, attributeName.length() - booleanSuffix.length()).trim();
            }
            String dateSuffix = "(" + Types.DATE + ")";
            if (attributeName.toLowerCase().endsWith(dateSuffix.toLowerCase())) {
                attributeType = Types.DATE;
                attributeName = attributeName.substring(0, attributeName.length() - dateSuffix.length()).trim();
            }
            Attribute attribute = new Attribute(tableName.trim(), attributeName.trim(), attributeType);
            attributes.add(attribute);
        }
        return attributes;
    }
}
