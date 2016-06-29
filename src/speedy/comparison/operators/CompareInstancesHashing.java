package speedy.comparison.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.SpeedyConstants;
import speedy.comparison.ComparisonConfiguration;
import speedy.comparison.ComparisonUtility;
import speedy.comparison.InstanceMatchTask;
import speedy.comparison.SignatureAttributes;
import speedy.comparison.SignatureMap;
import speedy.comparison.SignatureMapCollection;
import speedy.comparison.TupleMapping;
import speedy.comparison.TupleMatch;
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
    private final SignatureMapCollectionGenerator signatureGenerator = new SignatureMapCollectionGenerator();
    private final CheckTupleMatch tupleMatcher = new CheckTupleMatch();

    public InstanceMatchTask compare(IDatabase leftDb, IDatabase rightDb) {
        InstanceMatchTask instanceMatch = new InstanceMatchTask(leftDb, rightDb);
        long start = System.currentTimeMillis();
        List<TupleWithTable> leftTuples = SpeedyUtility.extractAllTuplesFromDatabase(leftDb);
        List<TupleWithTable> rightTuples = SpeedyUtility.extractAllTuplesFromDatabase(rightDb);
        SignatureMapCollection leftSignatureMapCollection = signatureGenerator.generateIndexForTuples(leftTuples);
        if (logger.isDebugEnabled()) logger.debug("Left Signature Map Collection:\n" + leftSignatureMapCollection);
        List<TupleWithTable> remainingRightTuples = new ArrayList<TupleWithTable>();
        TupleMapping ltrMapping = findMapping(leftSignatureMapCollection, rightTuples, remainingRightTuples);
        if (logger.isDebugEnabled()) logger.debug("LTR Mapping:\n" + ltrMapping);
        findRTLMapping(ltrMapping, leftSignatureMapCollection, rightTuples, remainingRightTuples);
        instanceMatch.setTupleMapping(ltrMapping);
        long end = System.currentTimeMillis();
        if (logger.isInfoEnabled()) logger.info("** Total time:" + (end - start) + " ms");
        return instanceMatch;
    }

    private TupleMapping findMapping(SignatureMapCollection srcSignatureMap, List<TupleWithTable> destTuples, List<TupleWithTable> extraTuples) {
        long start = System.currentTimeMillis();
        TupleMapping tupleMapping = new TupleMapping();
        tupleMapping.setScore(0.0);
        for (TupleWithTable destTuple : destTuples) {
            List<SignatureAttributes> signatureAttributesForTable = srcSignatureMap.getRankedAttributesForTable(destTuple.getTable());
            if (logger.isDebugEnabled()) logger.debug("Signature for table " + destTuple.getTable() + ": " + signatureAttributesForTable);
            List<TupleMatch> matchingTuples = findMatchingTuples(destTuple, signatureAttributesForTable, srcSignatureMap, tupleMapping);
            if (matchingTuples.isEmpty()) {
                if (logger.isDebugEnabled()) logger.debug("Extra tuple in dest instance: " + destTuple);
                extraTuples.add(destTuple);
                continue;
            }
            for (TupleMatch matchingTuple : matchingTuples) {
                addValueMapping(matchingTuple.getLeftTuple(), matchingTuple.getRightTuple(), tupleMapping.getLeftToRightValueMapping());
                tupleMapping.putTupleMapping(matchingTuple.getLeftTuple(), matchingTuple.getRightTuple());
                tupleMapping.addScore(matchingTuple.getSimilarity());
            }
        }
        long end = System.currentTimeMillis();
        if (logger.isInfoEnabled()) logger.info("Finding mapping time:" + (end - start) + " ms");
        return tupleMapping;
    }

    private List<TupleMatch> findMatchingTuples(TupleWithTable rightTuple, List<SignatureAttributes> signatureAttributesForTable,
            SignatureMapCollection leftSignatureMaps, TupleMapping tupleMapping) {
        List<TupleMatch> matchingTuples = new ArrayList<TupleMatch>();
        if (logger.isDebugEnabled()) logger.debug("Finding matching tuple for " + rightTuple);
        Set<AttributeRef> attributesWithGroundValues = ComparisonUtility.findAttributesWithGroundValue(rightTuple.getTuple());
        for (SignatureAttributes signatureAttribute : signatureAttributesForTable) {
            if (logger.isTraceEnabled()) logger.trace("Checking signature attribute " + signatureAttribute);
            if (!isCompatible(attributesWithGroundValues, signatureAttribute.getAttributes())) {
                if (logger.isTraceEnabled()) logger.trace("Skipping not compatible signature attribute " + signatureAttribute);
                continue;
            }
            SignatureMap signatureMap = leftSignatureMaps.getSignatureForAttributes(signatureAttribute);
            TupleSignature rightTupleSignature = signatureGenerator.generateSignature(rightTuple, signatureAttribute.getAttributes());
            List<Tuple> tuplesWithSameSignature = signatureMap.getTuplesForSignature(rightTupleSignature.getSignature());
            if (tuplesWithSameSignature == null || tuplesWithSameSignature.isEmpty()) {
                continue;
            }
            for (Iterator<Tuple> it = tuplesWithSameSignature.iterator(); it.hasNext();) {
                Tuple srcTuple = it.next();
                TupleWithTable srcTupleWithTable = new TupleWithTable(rightTuple.getTable(), srcTuple);
                TupleMatch tupleMatch = tupleMatcher.checkMatch(srcTupleWithTable, rightTuple);
                if (tupleMatch == null) {
                    continue;
                }
                if (!ComparisonUtility.isTupleMatchCompatibleWithTupleMapping(tupleMapping, tupleMatch)) {
                    continue;
                }
                ComparisonUtility.updateTupleMapping(tupleMapping, tupleMatch);
                matchingTuples.add(tupleMatch);
                it.remove();
                if (ComparisonConfiguration.isInjective()) {
                    return matchingTuples;
                }
            }
        }
        return matchingTuples;
    }

    private boolean isCompatible(Set<AttributeRef> attributesWithGroundValues, List<AttributeRef> attributes) {
        return attributesWithGroundValues.containsAll(attributes);
    }

    private void addValueMapping(TupleWithTable srcTuple, TupleWithTable destTuple, ValueMapping valueMapping) {
        for (Cell cell : srcTuple.getTuple().getCells()) {
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
            IValue dstValue = destTuple.getTuple().getCell(cell.getAttributeRef()).getValue();
            valueMapping.putValueMapping(srcValue, dstValue);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////
    ////////      RIGHT TO LEFT
    //////////////////////////////////////////////////////////////////////////////////////////
    private void findRTLMapping(TupleMapping ltrMapping, SignatureMapCollection leftSignatureMapCollection, List<TupleWithTable> rightTuples, List<TupleWithTable> remainingRightTuples) {
        if (logger.isDebugEnabled()) logger.debug("Finding RTL Mapping...");
        Map<TupleWithTable, TupleWithTable> renamedTupleMap = new HashMap<TupleWithTable, TupleWithTable>();
        List<TupleWithTable> leftTuplesToMatch = findLeftTuplesToMatch(ltrMapping, leftSignatureMapCollection, renamedTupleMap);
        if (logger.isDebugEnabled()) logger.debug("Left tuples to match:\n" + SpeedyUtility.printCollection(leftTuplesToMatch));
        List<TupleWithTable> rightTuplesToMatch = findRightTuplesToMatch(rightTuples, remainingRightTuples);
        if (logger.isDebugEnabled()) logger.debug("Right tuples to match:\n" + SpeedyUtility.printCollection(rightTuplesToMatch));
        SignatureMapCollection rightSignatureMapCollection = signatureGenerator.generateIndexForTuples(rightTuplesToMatch);
        if (logger.isDebugEnabled()) logger.debug("Right Signature Map Collection:\n" + leftSignatureMapCollection);
        List<TupleWithTable> remainingLeftTuples = new ArrayList<TupleWithTable>();
        TupleMapping rtlMapping = findMapping(rightSignatureMapCollection, leftTuplesToMatch, remainingLeftTuples);
        if (logger.isDebugEnabled()) logger.debug("RTL Mapping:\n" + rtlMapping);
        mergeMappings(ltrMapping, rtlMapping, renamedTupleMap);
        ltrMapping.setNonMatchingTuples(remainingLeftTuples);
    }

    private List<TupleWithTable> findLeftTuplesToMatch(TupleMapping tupleMapping, SignatureMapCollection leftSignatureMapCollection,
            Map<TupleWithTable, TupleWithTable> renamedTupleMap) {
        List<TupleWithTable> originalRemainingLeftTuples = collectRemainingTuples(leftSignatureMapCollection);
        List<TupleWithTable> renamedLeftTuples = new ArrayList<TupleWithTable>();
        for (TupleWithTable originalLeftTuple : originalRemainingLeftTuples) {
            TupleWithTable renamedTuple = applyValueMapping(originalLeftTuple, tupleMapping.getLeftToRightValueMapping());
            renamedLeftTuples.add(renamedTuple);
            renamedTupleMap.put(renamedTuple, originalLeftTuple);
        }
        return renamedLeftTuples;
    }

    private TupleWithTable applyValueMapping(TupleWithTable originalTuple, ValueMapping valueMapping) {
        Tuple tuple = new Tuple(originalTuple.getTuple().getOid());
        TupleWithTable renamedTuple = new TupleWithTable(originalTuple.getTable(), tuple);
        for (Cell cell : originalTuple.getTuple().getCells()) {
            if (cell.getAttribute().equals(SpeedyConstants.OID)) {
                tuple.addCell(cell);
                continue;
            }
            IValue originalValue = cell.getValue();
            IValue newValue = valueMapping.getValueMapping(originalValue);
            if (newValue == null) {
                tuple.addCell(cell);
                continue;
            }
            tuple.addCell(new Cell(cell, newValue));
        }
        return renamedTuple;
    }

    private List<TupleWithTable> findRightTuplesToMatch(List<TupleWithTable> rightTuples, List<TupleWithTable> remainingRightTuples) {
        if (ComparisonConfiguration.isInjective()) {
            return remainingRightTuples;
        } else {
            return rightTuples;
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

    private void mergeMappings(TupleMapping ltrMapping, TupleMapping rtlMapping, Map<TupleWithTable, TupleWithTable> renamedTupleMap) {
        for (TupleWithTable rightTuple : rtlMapping.getTupleMapping().keySet()) {
            TupleWithTable leftTuple = rtlMapping.getTupleMapping().get(rightTuple);
            TupleWithTable originalLeftTuple = renamedTupleMap.get(leftTuple);
            ltrMapping.getTupleMapping().put(originalLeftTuple, rightTuple);
        }
        for (IValue rightValue : rtlMapping.getLeftToRightValueMapping().getKeys()) {
            IValue leftValue = rtlMapping.getLeftToRightMappingForValue(rightValue);//TODO++ leftValue may be a renamed constant
            ltrMapping.addRightToLeftMappingForValue(rightValue, leftValue);
        }
        ltrMapping.addScore(rtlMapping.getScore()); //TODO++ rtlMapping score may include some mapping that are already in ltrMapping ???
    }

}
