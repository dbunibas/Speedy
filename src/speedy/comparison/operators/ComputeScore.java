package speedy.comparison.operators;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.SpeedyConstants;
import speedy.comparison.ComparisonConfiguration;
import speedy.comparison.ComparisonStats;
import speedy.comparison.TupleMapping;
import speedy.comparison.TupleWithTable;
import speedy.comparison.ValueMapping;
import speedy.comparison.ValueMappings;
import speedy.model.database.Cell;
import speedy.model.database.IValue;
import speedy.utility.SpeedyUtility;

public class ComputeScore {

    private final static Logger logger = LoggerFactory.getLogger(ComputeScore.class);
    private final static CheckTupleMatch tupleMatcher = new CheckTupleMatch();

    public double computeScore(List<TupleWithTable> leftTuples, List<TupleWithTable> rightTuples, TupleMapping tupleMapping) {
        long start = System.currentTimeMillis();
        if (logger.isDebugEnabled()) logger.debug("Computing score btw source tuples: \n " + SpeedyUtility.printCollection(leftTuples, "\t") + "\nand right tuples: \n " + SpeedyUtility.printCollection(rightTuples, "\t") + "\nwith mapping " + tupleMapping);
        Map<IValue, Integer> coveredByMap = computeCoveredByMap(tupleMapping.getValueMappings());
        if (logger.isDebugEnabled()) logger.debug("Covered by map:\n" + SpeedyUtility.printMap(coveredByMap));
        Map<TupleWithTable, Set<TupleWithTable>> directMapping = tupleMapping.getTupleMapping();
        Map<TupleWithTable, Set<TupleWithTable>> inverseMapping = computeInverseMapping(directMapping);
        double leftTupleScores = computeScoreForTuples(leftTuples, directMapping, coveredByMap, tupleMapping.getLeftToRightValueMapping());
        double rightTupleScores = computeScoreForTuples(rightTuples, inverseMapping, coveredByMap, tupleMapping.getRightToLeftValueMapping());
        if (logger.isDebugEnabled()) logger.debug("Left Tuple Score: " + leftTupleScores);
        if (logger.isDebugEnabled()) logger.debug("Right Tuple Score: " + rightTupleScores);
        if (logger.isDebugEnabled()) logger.debug("Number of Left Cells: " + numberOfCells(leftTuples));
        if (logger.isDebugEnabled()) logger.debug("Number of Right Cells: " + numberOfCells(rightTuples));
        double score = (leftTupleScores + rightTupleScores) / (numberOfCells(leftTuples) + numberOfCells(rightTuples));
        if (logger.isDebugEnabled()) logger.debug("Total Score: " + score);
        ComparisonStats.getInstance().addStat(ComparisonStats.COMPUTE_SCORE_TIME, System.currentTimeMillis() - start);
        return score;
    }

    private Map<IValue, Integer> computeCoveredByMap(ValueMappings valueMappings) {
        Map<IValue, Integer> result = new HashMap<IValue, Integer>();
        computeCoveredByMapForValueMapping(valueMappings.getLeftToRightValueMapping(), result);
        computeCoveredByMapForValueMapping(valueMappings.getRightToLeftValueMapping(), result);
        return result;
    }

    private void computeCoveredByMapForValueMapping(ValueMapping valueMapping, Map<IValue, Integer> result) {
        for (IValue invertedKey : valueMapping.getInvertedKeys()) {
            if (valueMapping.getInvertedValueMapping(invertedKey).isEmpty()) {
                continue;
            }
            result.put(invertedKey, valueMapping.getInvertedValueMapping(invertedKey).size());
        }
    }

    private Map<TupleWithTable, Set<TupleWithTable>> computeInverseMapping(Map<TupleWithTable, Set<TupleWithTable>> tupleMapping) {
        Map<TupleWithTable, Set<TupleWithTable>> result = new HashMap<TupleWithTable, Set<TupleWithTable>>();
        for (TupleWithTable leftTuple : tupleMapping.keySet()) {
            Set<TupleWithTable> rightTuples = tupleMapping.get(leftTuple);
            for (TupleWithTable rightTuple : rightTuples) {
                addTupleMapping(rightTuple, leftTuple, result);
            }
        }
        return result;
    }

    private void addTupleMapping(TupleWithTable rightTuple, TupleWithTable leftTuple, Map<TupleWithTable, Set<TupleWithTable>> result) {
        Set<TupleWithTable> mappedTuples = result.get(rightTuple);
        if (mappedTuples == null) {
            mappedTuples = new HashSet<TupleWithTable>();
            result.put(rightTuple, mappedTuples);
        }
        mappedTuples.add(leftTuple);
    }

    private double computeScoreForTuples(List<TupleWithTable> tuples, Map<TupleWithTable, Set<TupleWithTable>> mapping, Map<IValue, Integer> coveredByMap, ValueMapping valueMapping) {
        double scoreForTuples = 0.0;
        for (TupleWithTable tuple : tuples) {
            scoreForTuples += computeScoreForTuple(tuple, mapping, coveredByMap, valueMapping);
        }
        return scoreForTuples;
    }

    private double computeScoreForTuple(TupleWithTable tuple, Map<TupleWithTable, Set<TupleWithTable>> mapping, Map<IValue, Integer> coveredByMap, ValueMapping valueMapping) {
        Set<TupleWithTable> mappedTuples = mapping.get(tuple);
        if (mappedTuples == null || mappedTuples.isEmpty()) {
            if (logger.isDebugEnabled()) logger.debug("Tuple " + tuple + " is not mapped in mapping\n " + SpeedyUtility.printMap(mapping));
            return 0;
        }
        double sumTuplePair = 0.0;
        for (TupleWithTable mappedTuple : mappedTuples) {
            sumTuplePair += computeScoreForMapping(tuple, mappedTuple, coveredByMap, valueMapping);
        }
        return (sumTuplePair / (double) mappedTuples.size());
    }

    private double computeScoreForMapping(TupleWithTable srcTuple, TupleWithTable mappedTuple, Map<IValue, Integer> coveredByMap, ValueMapping valueMapping) {
        double score = 0.0;
        for (int i = 0; i < srcTuple.getTuple().getCells().size(); i++) {
            if (srcTuple.getTuple().getCells().get(i).getAttribute().equals(SpeedyConstants.OID)) {
                continue;
            }
            IValue srcValue = srcTuple.getTuple().getCells().get(i).getValue();
            IValue dstValue = mappedTuple.getTuple().getCells().get(i).getValue();
            double scoreForAttribute = scoreForAttribute(srcValue, dstValue, coveredByMap, valueMapping);
            if (logger.isDebugEnabled()) logger.debug("Score for value mapping: " + srcValue + " -> " + dstValue + ": " + scoreForAttribute);
            score += scoreForAttribute;
        }
        return score;
    }

    private double scoreForAttribute(IValue srcValue, IValue dstValue, Map<IValue, Integer> coveredByMap, ValueMapping valueMapping) {
        if (logger.isTraceEnabled()) logger.trace("Comparing values: '" + srcValue + "', '" + dstValue + "'");
        SpeedyConstants.ValueMatchResult matchResult = tupleMatcher.match(srcValue, dstValue);
        if (matchResult == SpeedyConstants.ValueMatchResult.NOT_MATCHING) {
            if (logger.isTraceEnabled()) logger.trace("Values not match...");
            return 0.0;
        }
        if (matchResult == SpeedyConstants.ValueMatchResult.EQUAL_CONSTANTS) {
            return 1.0;
        }
        if (matchResult == SpeedyConstants.ValueMatchResult.PLACEHOLDER_TO_CONSTANT
                || matchResult == SpeedyConstants.ValueMatchResult.CONSTANT_TO_PLACEHOLDER) {
            return ComparisonConfiguration.getK();
        }
        //Else Placeholder -> Placeholder
        if (!SpeedyUtility.isPlaceholder(dstValue)) {
            throw new IllegalArgumentException("Expecting a placeholder for destination value " + dstValue);
        }
        Integer coveredByValue = coveredByMap.get(dstValue);
        if (coveredByValue == null) {
            coveredByValue = 1;
        }
        if (logger.isDebugEnabled()) logger.debug("Value " + dstValue + " is covered " + coveredByValue + " times");
        return 1 / (double) coveredByValue;
    }

    private int numberOfCells(List<TupleWithTable> tuples) {
        if (tuples.isEmpty()) {
            return 0;
        }
        Map<String, Integer> attributesForTable = new HashMap<String, Integer>();
        int numberOfCells = 0;
        for (TupleWithTable tuple : tuples) {
            numberOfCells += getNumberOfCellsForTable(tuple, attributesForTable);
        }
        return numberOfCells;
    }

    private int getNumberOfCellsForTable(TupleWithTable tuple, Map<String, Integer> attributesForTable) {
        String table = tuple.getTable();
        Integer attributes = attributesForTable.get(table);
        if (attributes != null) {
            return attributes;
        }
        int numberOfCells = 0;
        for (Cell cell : tuple.getTuple().getCells()) {
            if (cell.isOID()) {
                continue;
            }
            numberOfCells++;
        }
        attributesForTable.put(table, numberOfCells);
        return numberOfCells;
    }
}
