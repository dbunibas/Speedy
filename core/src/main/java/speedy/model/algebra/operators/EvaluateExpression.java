package speedy.model.algebra.operators;

import speedy.utility.AlgebraUtility;
import speedy.SpeedyConstants;
import speedy.exceptions.ExpressionSyntaxException;
import speedy.model.database.AttributeRef;
import speedy.model.database.Tuple;
import speedy.model.expressions.Expression;
import org.nfunk.jep.JEP;
import org.nfunk.jep.SymbolTable;
import org.nfunk.jep.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.database.IValue;
import speedy.model.database.IVariableDescription;
import speedy.model.database.NullValue;

public class EvaluateExpression {

    private static Logger logger = LoggerFactory.getLogger(EvaluateExpression.class);

    public Object evaluateFunction(Expression expression, Tuple tuple) throws ExpressionSyntaxException {
        if (logger.isDebugEnabled()) logger.debug("Evaluating function: " + expression + " on tuple " + tuple);
        setVariableValues(expression, tuple);
        Object value = expression.getJepExpression().getValueAsObject();
        if (expression.getJepExpression().hasError()) {
            throw new ExpressionSyntaxException(expression.getJepExpression().getErrorInfo());
        }
        if (logger.isDebugEnabled()) logger.debug("Value of function: " + value);
        return value;
    }

    public Double evaluateCondition(Expression expression, Tuple tuple) throws ExpressionSyntaxException {
        if (logger.isDebugEnabled()) logger.debug("Evaluating condition: " + expression + " on tuple " + tuple);
        if (expression.toString().equals("true")) {
            return SpeedyConstants.TRUE;
        }
        setVariableValues(expression, tuple);
        Object value = expression.getJepExpression().getValueAsObject();
        if (expression.getJepExpression().hasError()) {
            throw new ExpressionSyntaxException(expression.getJepExpression().getErrorInfo());
        }
        if (logger.isDebugEnabled()) logger.debug("Value of condition: " + value);
        try {
            Double result = Double.parseDouble(value.toString());
            return result;
        } catch (NumberFormatException numberFormatException) {
            logger.error(numberFormatException.toString());
            throw new ExpressionSyntaxException(numberFormatException.getMessage());
        }
    }

    private void setVariableValues(Expression expression, Tuple tuple) {
        if (logger.isDebugEnabled()) logger.debug("Evaluating expression " + expression.toLongString() + "\n on tuple " + tuple);
        JEP jepExpression = expression.getJepExpression();
        SymbolTable symbolTable = jepExpression.getSymbolTable();
        for (Variable jepVariable : symbolTable.getVariables()) {
            if (AlgebraUtility.isPlaceholder(jepVariable)) {
                continue;
            }
            Object variableDescription = jepVariable.getDescription();
            Object variableValue = findAttributeValue(tuple, variableDescription);
            assert (variableValue != null) : "Value of variable: " + jepVariable + " is null in tuple " + tuple;
            IValue cellValue = findValueForAttribute(tuple, variableDescription);
            if (cellValue instanceof NullValue
                    && !expression.getExpressionString().toLowerCase().contains("not null")
                    && !expression.getExpressionString().toLowerCase().contains("is null")) continue; // TODO: is that true ? check it
            if (logger.isTraceEnabled()) logger.trace("Setting var value: " + jepVariable.getDescription() + " = " + variableValue);
            jepExpression.setVarValue(jepVariable.getName(), variableValue);
        }
    }

    private Object findAttributeValue(Tuple tuple, Object description) {
        if (logger.isTraceEnabled()) logger.trace("Searching variable: " + description + " in tuple " + tuple);
        AttributeRef attributeRef = null;
        if (description instanceof IVariableDescription) {
            IVariableDescription variableDescription = (IVariableDescription) description;
            attributeRef = findOccurrenceInTuple(variableDescription, tuple);
        } else if (description instanceof AttributeRef) {
            attributeRef = (AttributeRef) description;
        } else {
            throw new IllegalArgumentException("Illegal variable description in expression: " + description + " of type " + description.getClass().getName());
        }
        return AlgebraUtility.getCellValue(tuple, attributeRef).toString();
    }
    
    private IValue findValueForAttribute(Tuple tuple, Object description) {
        if (logger.isTraceEnabled()) logger.trace("Searching variable: " + description + " in tuple " + tuple);
        AttributeRef attributeRef = null;
        if (description instanceof IVariableDescription) {
            IVariableDescription variableDescription = (IVariableDescription) description;
            attributeRef = findOccurrenceInTuple(variableDescription, tuple);
        } else if (description instanceof AttributeRef) {
            attributeRef = (AttributeRef) description;
        } else {
            throw new IllegalArgumentException("Illegal variable description in expression: " + description + " of type " + description.getClass().getName());
        }
        return AlgebraUtility.getCellValue(tuple, attributeRef);
    }

    private AttributeRef findOccurrenceInTuple(IVariableDescription variableDescription, Tuple tuple) {
        for (AttributeRef attributeRef : variableDescription.getAttributeRefs()) {
            if (AlgebraUtility.contains(tuple, attributeRef)) {
                return attributeRef;
            }
        }
        throw new IllegalArgumentException("Unable to find values for variable " + variableDescription.toString() + " in tuple " + tuple);
    }

}
