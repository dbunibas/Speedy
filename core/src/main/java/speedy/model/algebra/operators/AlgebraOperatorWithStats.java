package speedy.model.algebra.operators;

import speedy.model.algebra.IAlgebraOperator;
import speedy.model.database.ResultInfo;

public class AlgebraOperatorWithStats {

    private IAlgebraOperator operator;
    private ResultInfo resultInfo;
    private boolean randomized = false;

    public AlgebraOperatorWithStats(IAlgebraOperator operator) {
        this.operator = operator;
    }

    public ResultInfo getResultInfo() {
        return resultInfo;
    }

    public void setResultInfo(ResultInfo resultInfo) {
        this.resultInfo = resultInfo;
    }

    public IAlgebraOperator getOperator() {
        return operator;
    }

    public boolean isRandomized() {
        return randomized;
    }

    public void setRandomized(boolean randomized) {
        this.randomized = randomized;
    }

    @Override
    public String toString() {
        return "Operator:\n" + operator + "\b\t" + resultInfo;
    }

}
