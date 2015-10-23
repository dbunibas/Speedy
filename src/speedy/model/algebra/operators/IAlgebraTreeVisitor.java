package speedy.model.algebra.operators;

import speedy.model.algebra.CartesianProduct;
import speedy.model.algebra.CreateTableAs;
import speedy.model.algebra.Difference;
import speedy.model.algebra.Distinct;
import speedy.model.algebra.ExtractRandomSample;
import speedy.model.algebra.GroupBy;
import speedy.model.algebra.Join;
import speedy.model.algebra.Limit;
import speedy.model.algebra.Offset;
import speedy.model.algebra.OrderBy;
import speedy.model.algebra.OrderByRandom;
import speedy.model.algebra.Partition;
import speedy.model.algebra.Project;
import speedy.model.algebra.RestoreOIDs;
import speedy.model.algebra.Scan;
import speedy.model.algebra.Select;
import speedy.model.algebra.SelectIn;
import speedy.model.algebra.Union;

public interface IAlgebraTreeVisitor {

    void visitScan(Scan operator);
    void visitSelect(Select operator);
    void visitDistinct(Distinct operator);
    void visitSelectIn(SelectIn operator);
    void visitJoin(Join operator);
    void visitCartesianProduct(CartesianProduct operator);
    void visitProject(Project operator);
    void visitDifference(Difference operator);
    void visitUnion(Union operator);
    void visitGroupBy(GroupBy operator);
    void visitPartition(Partition operator);
    void visitOrderBy(OrderBy operator);
    void visitOrderByRandom(OrderByRandom operator);
    void visitLimit(Limit operator);
    void visitOffset(Offset operator);
    void visitRestoreOIDs(RestoreOIDs operator);
    void visitCreateTable(CreateTableAs operator);
    void visitExtractRandomSample(ExtractRandomSample operator);
    Object getResult();
}
