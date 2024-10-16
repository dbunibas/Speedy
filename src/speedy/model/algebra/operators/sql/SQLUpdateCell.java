package speedy.model.algebra.operators.sql;

import speedy.SpeedyConstants;
import speedy.model.algebra.operators.IUpdateCell;
import speedy.model.database.Attribute;
import speedy.model.database.AttributeRef;
import speedy.model.database.CellRef;
import speedy.model.database.IDatabase;
import speedy.model.database.IValue;
import speedy.model.database.dbms.DBMSDB;
import speedy.persistence.Types;
import speedy.persistence.relational.QueryManager;
import speedy.utility.SpeedyUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.utility.DBMSUtility;

public class SQLUpdateCell implements IUpdateCell {
    
    private static Logger logger = LoggerFactory.getLogger(SQLUpdateCell.class);
    
    @Override
    public void execute(CellRef cellRef, IValue value, IDatabase database) {
        if (logger.isDebugEnabled()) logger.debug("Changing cell " + cellRef + " with new value " + value + " in database " + database);
        StringBuilder query = new StringBuilder();
        query.append("UPDATE ");
        AttributeRef attributeRef = cellRef.getAttributeRef();
        query.append(DBMSUtility.getSchemaNameAndDot(((DBMSDB) database).getAccessConfiguration()));
        query.append(cellRef.getAttributeRef().getTableName());
        query.append(" SET ").append(attributeRef.getName()).append("=");
        Attribute attribute = SpeedyUtility.getAttribute(attributeRef, database);
        if(attribute.getType().equals(Types.STRING)){
            query.append("'");
        }
        query.append(cleanValue(value.toString()));
        if(attribute.getType().equals(Types.STRING)){
            query.append("'");
        }
        query.append(" WHERE ").append(SpeedyConstants.OID).append("=");
        query.append(cellRef.getTupleOID());
        query.append(";");
        if (logger.isDebugEnabled()) logger.debug("Update script: \n" + query.toString());
        QueryManager.executeScript(query.toString(), ((DBMSDB) database).getAccessConfiguration(), true, true, false, false);
    }
    
    private String cleanValue(String string) {
        String sqlValue = string;
        sqlValue = sqlValue.replaceAll("'", "''");
        sqlValue = SpeedyUtility.cleanConstantValue(sqlValue);
        return sqlValue;
    }
}
