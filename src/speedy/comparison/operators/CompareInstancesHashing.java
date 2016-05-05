package speedy.comparison.operators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.SpeedyConstants;
import speedy.comparison.ComparisonUtility;
import speedy.comparison.InstanceMatch;
import speedy.comparison.SignatureAttributes;
import speedy.comparison.SignatureMap;
import speedy.comparison.SignatureMapCollection;
import speedy.comparison.TupleMapping;
import speedy.comparison.TupleSignature;
import speedy.comparison.TupleWithTable;
import speedy.comparison.ValueMapping;
import speedy.model.database.AttributeRef;
import speedy.model.database.Cell;
import speedy.model.database.ConstantValue;
import speedy.model.database.IDatabase;
import speedy.model.database.IValue;
import speedy.model.database.Tuple;
import speedy.utility.SpeedyUtility;

public class CompareInstancesHashing implements IComputeInstanceSimilarity {

    private final static Logger logger = LoggerFactory.getLogger(CompareInstancesHashing.class);
    private final static SignatureMapCollectionGenerator signatureGenerator = new SignatureMapCollectionGenerator();

    public InstanceMatch compare(IDatabase leftDb, IDatabase rightDb) {
        InstanceMatch instanceMatch = new InstanceMatch(leftDb, rightDb);
        List<TupleWithTable> leftTuples = SpeedyUtility.extractAllTuplesFromDatabase(leftDb);
        List<TupleWithTable> rightTuples = SpeedyUtility.extractAllTuplesFromDatabase(rightDb);
        long start = System.currentTimeMillis();
        SignatureMapCollection leftSignatureMapCollection = signatureGenerator.generateIndexForTuples(leftTuples);
        long end = System.currentTimeMillis();
        if (logger.isInfoEnabled()) logger.info("Generate SignatureMapCollection time:" + (end - start) + " ms");
        if (logger.isDebugEnabled()) logger.debug("Left Signature Map Collection:\n" + leftSignatureMapCollection);
        List<TupleWithTable> remainingRightTuples = new ArrayList<TupleWithTable>();
        TupleMapping tupleMapping = findMapping(leftSignatureMapCollection, rightTuples, remainingRightTuples);
        List<TupleWithTable> remainingLeftTuples = collectRemainingTuples(leftSignatureMapCollection);
        if (logger.isDebugEnabled()) logger.debug("Remaining left tuples:\n" + SpeedyUtility.printCollection(remainingLeftTuples));
        //compute score
        instanceMatch.setTupleMatch(tupleMapping);
        return instanceMatch;
    }

    private TupleMapping findMapping(SignatureMapCollection srcSignatureMap, List<TupleWithTable> destTuples, List<TupleWithTable> extraTuples) {
        TupleMapping tupleMapping = new TupleMapping();
        for (TupleWithTable destTuple : destTuples) {
            List<SignatureAttributes> signatureAttributesForTable = srcSignatureMap.getRankedAttributesForTable(destTuple.getTable());
            if (logger.isDebugEnabled()) logger.debug("Signature for table " + destTuple.getTable() + ": " + signatureAttributesForTable);
            TupleWithTable matchingTuple = findMatchingTuple(destTuple, signatureAttributesForTable, srcSignatureMap, tupleMapping);
            if (matchingTuple == null) {
                if (logger.isDebugEnabled()) logger.debug("Extra tuple in dest instance: " + destTuple);
                extraTuples.add(destTuple);
                continue;
            }
            addValueMapping(matchingTuple.getTuple(), destTuple.getTuple(), tupleMapping.getLeftToRightValueMapping());
            tupleMapping.putTupleMapping(matchingTuple, destTuple);
        }
        return tupleMapping;
    }

    private TupleWithTable findMatchingTuple(TupleWithTable destTuple, List<SignatureAttributes> signatureAttributesForTable,
            SignatureMapCollection srcSignatureMapCollection, TupleMapping tupleMapping) {
        if (logger.isDebugEnabled()) logger.debug("Finding matching tuple for " + destTuple);
        String tableName = destTuple.getTable();
        Set<AttributeRef> attributesWithGroundValues = ComparisonUtility.findAttributesWithGroundValue(destTuple.getTuple());
        for (SignatureAttributes signatureAttribute : signatureAttributesForTable) {
            if (logger.isTraceEnabled()) logger.trace("Checking signature attribute " + signatureAttribute);
            if (!isCompatible(attributesWithGroundValues, signatureAttribute.getAttributes())) {
                if (logger.isTraceEnabled()) logger.trace("Skipping not compatible signature attribute " + signatureAttribute);
                continue;
            }
            SignatureMap signatureMap = srcSignatureMapCollection.getSignatureForAttributes(signatureAttribute);
            TupleSignature rightTupleSignature = signatureGenerator.generateSignature(destTuple, signatureAttribute.getAttributes());
            List<Tuple> tuplesWithSameSignature = signatureMap.getTuplesForSignature(rightTupleSignature.getSignature());
            if (tuplesWithSameSignature.isEmpty()) {
                continue;
            }
            for (Iterator<Tuple> it = tuplesWithSameSignature.iterator(); it.hasNext();) {
                Tuple srcTuple = it.next();
                if (!isValueMappingCompatible(srcTuple, destTuple.getTuple(), tupleMapping.getLeftToRightValueMapping())) {
                    continue;
                }
                it.remove();
                return new TupleWithTable(tableName, srcTuple);
            }
        }
        return null;
    }

    private boolean isCompatible(Set<AttributeRef> attributesWithGroundValues, List<AttributeRef> attributes) {
        return attributesWithGroundValues.containsAll(attributes);
    }

    private boolean isValueMappingCompatible(Tuple srcTuple, Tuple destTuple, ValueMapping valueMapping) {
        for (Cell cell : srcTuple.getCells()) {
            if (cell.getAttribute().equals(SpeedyConstants.OID)) {
                continue;
            }
            IValue srcValue = cell.getValue();
            if ((srcValue instanceof ConstantValue)) {
                continue;
            }
            IValue mappedValue = valueMapping.getValueMapping(srcValue);
            if (mappedValue == null) {
                continue;
            }
            IValue dstValue = destTuple.getCell(cell.getAttributeRef()).getValue();
            if (!mappedValue.equals(dstValue)) {
                if (logger.isDebugEnabled()) logger.debug("Not compatible mapping: " + srcTuple + " - " + destTuple + " with mapping " + valueMapping.toString());
                return false;
            }
        }
        return true;
    }

    private void addValueMapping(Tuple srcTuple, Tuple destTuple, ValueMapping valueMapping) {
        for (Cell cell : srcTuple.getCells()) {
            if (cell.getAttribute().equals(SpeedyConstants.OID)) {
                continue;
            }
            IValue srcValue = cell.getValue();
            if ((srcValue instanceof ConstantValue)) {
                continue;
            }
            IValue existingMapping = valueMapping.getValueMapping(srcValue);
            if (existingMapping != null) {
                continue;
            }
            IValue dstValue = destTuple.getCell(cell.getAttributeRef()).getValue();
            valueMapping.putValueMapping(srcValue, dstValue);
        }
    }

    private List<TupleWithTable> collectRemainingTuples(SignatureMapCollection signatureMapCollection) {
        List<TupleWithTable> result = new ArrayList<TupleWithTable>();
        for (SignatureMap signatureMap : signatureMapCollection.getSignatureMaps()) {
            String tableName = signatureMap.getSignatureAttribute().getTableName();
            for (List<Tuple> tuples : signatureMap.getIndex().values()) {
                for (Tuple tuple : tuples) {
                    result.add(new TupleWithTable(tableName, tuple));
                }
            }
        }
        return result;
    }

//    private void compareTable(ITable leftTable, ITable rightTable, SimilarityResult result) {
//        List<TupleMatch> tupleMatches = new ArrayList<TupleMatch>();
//        Set<TupleOID> matchedExpected = new HashSet<TupleOID>();
//        Set<TupleOID> matchedGenerated = new HashSet<TupleOID>();
//        findExactMatches(leftTable, rightTable, tupleMatches, matchedExpected, matchedGenerated);
//        TableSimilarity similarity = computeSimilarity(tupleMatches, leftTable, rightTable);
//        if (logger.isDebugEnabled()) logger.debug("Similarity for table " + leftTable.getName() + ": " + similarity);
//        result.setTableSimilarity(leftTable.getName(), similarity);
//    }
//
//    private void findExactMatches(ITable leftTable, ITable rightTable, List<TupleMatch> tupleMatches, Set<TupleOID> matchedExpected, Set<TupleOID> matchedGenerated) {
//    }
//
//    private TableSimilarity computeSimilarity(List<TupleMatch> tupleMatches, ITable expectedTable, ITable generatedTable) {
//        double totalSimilarity = 0.0;
//        for (TupleMatch match : tupleMatches) {
//            totalSimilarity += match.getSimilarity();
//        }
//        double precision = totalSimilarity / (double) generatedTable.getSize();
//        double recall = totalSimilarity / (double) expectedTable.getSize();
//        return new TableSimilarity(totalSimilarity, precision, recall);
//    }
}
