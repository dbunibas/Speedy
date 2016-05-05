package speedy.comparison.operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.comparison.ComparisonUtility;
import speedy.comparison.SignatureAttributes;
import speedy.comparison.SignatureMap;
import speedy.comparison.SignatureMapCollection;
import speedy.comparison.TupleSignature;
import speedy.comparison.TupleWithTable;
import speedy.model.database.AttributeRef;
import speedy.model.database.Cell;
import speedy.model.database.Tuple;
import speedy.utility.SpeedyUtility;
import speedy.utility.comparator.StringComparator;

public class SignatureMapCollectionGenerator {

    private final static Logger logger = LoggerFactory.getLogger(SignatureMapCollectionGenerator.class);
    private final static String SIGNATURE_SEPARATOR = "|";

    public SignatureMapCollection generateIndexForTuples(List<TupleWithTable> tuples) {
        SignatureMapCollection signatureCollection = new SignatureMapCollection();
        for (TupleWithTable tupleWithTable : tuples) {
            Set<AttributeRef> groundAttributes = ComparisonUtility.findAttributesWithGroundValue(tupleWithTable.getTuple());
            TupleSignature maximalSignature = generateSignature(tupleWithTable, groundAttributes);
            if (logger.isDebugEnabled()) logger.debug("Signature: " + maximalSignature);
            SignatureMap signatureMap = signatureCollection.getOrCreateSignatureMap(maximalSignature.getSignatureAttribute());
            signatureMap.addSignature(maximalSignature);
        }
        return signatureCollection;
    }

    public TupleSignature generateSignature(TupleWithTable tupleWithTable, Collection<AttributeRef> attributes) {
        List<AttributeRef> sortedAttributes = new ArrayList<AttributeRef>(attributes);
        Collections.sort(sortedAttributes, new StringComparator());
        Tuple tuple = tupleWithTable.getTuple();
        String tableName = tupleWithTable.getTable();
        StringBuilder signature = new StringBuilder();
        for (AttributeRef attribute : sortedAttributes) {
            Cell cell = tuple.getCell(attribute);
            signature.append(cell.getValue()).append(SIGNATURE_SEPARATOR);
        }
        SpeedyUtility.removeChars(SIGNATURE_SEPARATOR.length(), signature);
        SignatureAttributes signatureAttribute = new SignatureAttributes(tableName, sortedAttributes);
        return new TupleSignature(tuple, signatureAttribute, signature.toString());
    }
}
