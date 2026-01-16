package speedy.model.algebra;

import speedy.model.algebra.aggregatefunctions.IAggregateFunction;
import speedy.model.algebra.udf.IUserDefinedFunction;
import speedy.model.algebra.udf.UserDefinedAttributeRef;
import speedy.model.algebra.udf.UserDefinedFunction;
import speedy.model.database.*;
import speedy.utility.SpeedyUtility;
import speedy.model.algebra.operators.ListTupleIterator;
import speedy.model.algebra.operators.IAlgebraTreeVisitor;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.mainmemory.datasource.IntegerOIDGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.utility.comparator.TupleComparatorOIDs;

public class GroupBy extends AbstractOperator {

    private static Logger logger = LoggerFactory.getLogger(GroupBy.class);

    private List<AttributeRef> groupingAttributes;
    private List<IAggregateFunction> aggregateFunctions;

    public GroupBy(List<AttributeRef> groupingAttributes, List<IAggregateFunction> aggregateFunctions) {
        this.groupingAttributes = groupingAttributes;
        this.aggregateFunctions = aggregateFunctions;
    }

    public String getName() {
        return "GROUP-BY-" + groupingAttributes + " - SELECT " + aggregateFunctions;
    }

    public void accept(IAlgebraTreeVisitor visitor) {
        visitor.visitGroupBy(this);
    }

    public ITupleIterator execute(IDatabase source, IDatabase target) {
        if (logger.isDebugEnabled()) logger.debug("Executing group-by: {} on source\n{}\nand target:\n{}", getName(), source == null ? "" : source.printInstances(), target.printInstances());
        List<Tuple> result = new ArrayList<Tuple>();
        ITupleIterator originalTuples = children.get(0).execute(source, target);
        materializeResult(target, originalTuples, result);
        Collections.sort(result, new TupleComparatorOIDs());
        originalTuples.close();
        if (logger.isDebugEnabled()) logger.debug("Result:\n" + SpeedyUtility.printCollection(result));
        return new ListTupleIterator(result);
    }

    private void materializeResult(IDatabase db, ITupleIterator originalTuples, List<Tuple> result) {
        List<Tuple> newTuples = generateUDFAttributes(originalTuples);
        Map<String, List<Tuple>> groups = groupTuples(new ListTupleIterator(newTuples));
        for (List<Tuple> group : groups.values()) {
            Tuple tuple = new Tuple(new TupleOID(IntegerOIDGenerator.getNextOID()));
            for (IAggregateFunction function : aggregateFunctions) {
                IValue aggregateValue = function.evaluate(db, group);
                Cell cell = new Cell(tuple.getOid(), function.getNewAttributeRef(), aggregateValue);
                tuple.addCell(cell);
            }
            result.add(tuple);
        }
        logger.trace("GroupBy Result: {}", result);
    }

    private List<Tuple> generateUDFAttributes(ITupleIterator originalTuples) {
        List<UserDefinedAttributeRef> userDefinedAttributeRefs = new ArrayList<>();
        for (IAggregateFunction function : aggregateFunctions) {
            if (!(function.getAttributeRef() instanceof UserDefinedAttributeRef udfRef)) continue;
            userDefinedAttributeRefs.add(udfRef);
        }

        List<Tuple> newTuples = new ArrayList<>();
        while (originalTuples.hasNext()) {
            Tuple tuple = originalTuples.next().clone();
            for (UserDefinedAttributeRef udfAttributeRef : userDefinedAttributeRefs) {
                IUserDefinedFunction userDefinedFunction = udfAttributeRef.getUserDefinedFunction();
                Object value = userDefinedFunction.execute(tuple);
                Cell cell = new Cell(tuple.getOid(), udfAttributeRef, new ConstantValue(value));
                tuple.addCell(cell);
            }
            newTuples.add(tuple);
        }

        return newTuples;
    }

    public static List<Object> getTupleValues(Tuple tuple, List<AttributeRef> attributes) {
        List<Object> values = new ArrayList<Object>();
        for (AttributeRef attribute : attributes) {
            values.add(tuple.getCell(attribute).getValue());
        }
        return values;
    }

    public List<AttributeRef> getAttributes(IDatabase source, IDatabase target) {
        return this.groupingAttributes;
    }

    private String generateKey(List<Object> tupleValues) {
        StringBuilder result = new StringBuilder("|");
        for (Object value : tupleValues) {
            result.append(value).append("|");
        }
        return result.toString();
    }

    private Map<String, List<Tuple>> groupTuples(ITupleIterator originalTuples) {
        Map<String, List<Tuple>> groups = new HashMap<String, List<Tuple>>();
        while (originalTuples.hasNext()) {
            Tuple originalTuple = originalTuples.next();
            List<Object> tupleValues = getTupleValues(originalTuple, groupingAttributes);
            String key = generateKey(tupleValues);
            List<Tuple> groupForKey = groups.get(key);
            if (groupForKey == null) {
                groupForKey = new ArrayList<Tuple>();
                groups.put(key, groupForKey);
            }
            groupForKey.add(originalTuple);
        }
        return groups;
    }

    public List<AttributeRef> getGroupingAttributes() {
        return groupingAttributes;
    }

    public List<IAggregateFunction> getAggregateFunctions() {
        return aggregateFunctions;
    }
}
