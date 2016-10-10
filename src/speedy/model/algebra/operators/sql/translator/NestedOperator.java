package speedy.model.algebra.operators.sql.translator;

import speedy.model.algebra.IAlgebraOperator;
import speedy.model.database.TableAlias;

public class NestedOperator {

    private IAlgebraOperator operator;
    private TableAlias alias;

    public NestedOperator(IAlgebraOperator operator, TableAlias alias) {
        this.operator = operator;
        this.alias = alias;
    }

    @Override
    public String toString() {
        return alias.toString();
    }

    public IAlgebraOperator getOperator() {
        return operator;
    }

    public TableAlias getAlias() {
        return alias;
    }
    
    

}
