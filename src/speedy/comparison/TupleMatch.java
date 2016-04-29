package speedy.comparison;

import java.text.DecimalFormat;

public class TupleMatch {

    private final static DecimalFormat df = new DecimalFormat("#0.00");
    private TupleWithTable leftTuple;
    private TupleWithTable rightTuple;
    private ValueMapping valueMapping;
    private Double similarity;

    public TupleMatch(TupleWithTable leftTuple, TupleWithTable rightTuple, Double similarity) {
        this.leftTuple = leftTuple;
        this.rightTuple = rightTuple;
        this.similarity = similarity;
    }

    public TupleMatch(TupleWithTable leftTuple, TupleWithTable rightTuple, ValueMapping valueMapping, double similarity) {
        this(leftTuple, rightTuple, similarity);
        this.valueMapping = valueMapping;
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

    public ValueMapping getValueMapping() {
        return valueMapping;
    }

    @Override
    public String toString() {
        return "Match: (" + df.format(similarity) + ") " + leftTuple.toString() + " <-> " + rightTuple.toString() +
                (this.valueMapping != null ? "\n" + valueMapping : "");
    }
}
