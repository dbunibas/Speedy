package speedy.comparison;

import java.text.DecimalFormat;

public class TupleMatch {

    private final static DecimalFormat df = new DecimalFormat("#0.00");
    private TupleWithTable leftTuple;
    private TupleWithTable rightTuple;
    private ValueMapping leftToRightValueMapping;
    private ValueMapping rightToLeftValueMapping;
    private Double similarity;

    public TupleMatch(TupleWithTable leftTuple, TupleWithTable rightTuple, Double similarity) {
        this.leftTuple = leftTuple;
        this.rightTuple = rightTuple;
        this.similarity = similarity;
    }

    public TupleMatch(TupleWithTable leftTuple, TupleWithTable rightTuple, ValueMapping valueMapping, double similarity) {
        this(leftTuple, rightTuple, similarity);
        this.leftToRightValueMapping = valueMapping;
    }

    public TupleWithTable getLeftTuple() {
        return leftTuple;
    }

    public TupleWithTable getRightTuple() {
        return rightTuple;
    }

    public Double getSimilarity() {
        return similarity;
    }

    public ValueMapping getLeftToRightValueMapping() {
        return leftToRightValueMapping;
    }

    public ValueMapping getRightToLeftValueMapping() {
        return rightToLeftValueMapping;
    }

    public void setRightToLeftValueMapping(ValueMapping rightToLeftValueMapping) {
        this.rightToLeftValueMapping = rightToLeftValueMapping;
    }

    @Override
    public String toString() {
        return "Match: (" + df.format(similarity) + ") " + leftTuple.toString() + " <-> " + rightTuple.toString() +
                (this.leftToRightValueMapping != null ? "\n" + leftToRightValueMapping : "") +
                (this.rightToLeftValueMapping != null ? "\n" + rightToLeftValueMapping : "");
    }
}
