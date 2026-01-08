package speedy.model.algebra.udf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.algebra.AbstractOperator;
import speedy.model.algebra.operators.IAlgebraTreeVisitor;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.AttributeRef;
import speedy.model.database.IDatabase;

import java.util.List;

public class UserDefinedFunction extends AbstractOperator {

    private static Logger logger = LoggerFactory.getLogger(UserDefinedFunction.class);

    private IUserDefinedFunction function;

    public UserDefinedFunction(IUserDefinedFunction function) {
        this.function = function;
    }

    public IUserDefinedFunction getFunction() {
        return function;
    }

    @Override
    public String getName() {
        return "USER_DEFINED_FUNCTION-[(" + function.getClass().getSimpleName() + ")]";
    }

    @Override
    public ITupleIterator execute(IDatabase source, IDatabase target) {
        ITupleIterator iterator = this.children.get(0).execute(source, target);
        return this.function.execute(iterator);
    }

    @Override
    public List<AttributeRef> getAttributes(IDatabase source, IDatabase target) {
        return this.children.get(0).getAttributes(source, target);
    }

    @Override
    public void accept(IAlgebraTreeVisitor visitor) {
        visitor.visitUserDefinedFunction(this);
    }

}
