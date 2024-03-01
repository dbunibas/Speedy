package speedy.model.algebra;

import speedy.model.algebra.operators.IAlgebraTreeVisitor;
import speedy.model.algebra.operators.ITupleIterator;
import speedy.model.database.AttributeRef;
import speedy.model.database.IDatabase;
import java.util.List;

public interface IAlgebraOperator extends Cloneable {

    public void setFather(IAlgebraOperator father);

    public IAlgebraOperator getFather();

    public void addChild(IAlgebraOperator child);

    public List<IAlgebraOperator> getChildren();

    public String getName();

    public ITupleIterator execute(IDatabase source, IDatabase target);

    public List<AttributeRef> getAttributes(IDatabase source, IDatabase target);

    public String toString(String indent);

    public void accept(IAlgebraTreeVisitor visitor);

    public IAlgebraOperator clone();

}
