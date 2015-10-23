package speedy.model.algebra;

import speedy.model.algebra.operators.ListTupleIterator;
import speedy.model.algebra.operators.IAlgebraTreeVisitor;
import speedy.model.algebra.operators.ITupleIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.database.AttributeRef;
import speedy.model.database.IDatabase;
import speedy.model.database.Tuple;

public class OrderByRandom extends AbstractOperator {

    private static Logger logger = LoggerFactory.getLogger(OrderByRandom.class);

    public OrderByRandom() {
    }

    public String getName() {
        return "ORDER BY RANDOM";
    }

    public void accept(IAlgebraTreeVisitor visitor) {
        visitor.visitOrderByRandom(this);
    }

    public ITupleIterator execute(IDatabase source, IDatabase target) {
        List<Tuple> result = new ArrayList<Tuple>();
        ITupleIterator originalTuples = children.get(0).execute(source, target);
        materializeResult(originalTuples, result);
        Collections.shuffle(result);
        originalTuples.close();
        return new ListTupleIterator(result);
    }

    private void materializeResult(ITupleIterator originalTuples, List<Tuple> result) {
        while (originalTuples.hasNext()) {
            Tuple originalTuple = originalTuples.next();
            result.add(originalTuple);
        }
    }

    public List<AttributeRef> getAttributes(IDatabase source, IDatabase target) {
        return this.children.get(0).getAttributes(source, target);
    }
}
