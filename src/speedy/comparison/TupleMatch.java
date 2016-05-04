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

    public TupleMatch(TupleWithTable leftTuple, TupleWithTable rightTuple, ValueMapping leftToRightValueMapping, ValueMapping rightToLeftValueMapping, double similarity) {
        this(leftTuple, rightTuple, similarity);
        this.leftToRightValueMapping = leftToRightValueMapping;
        this.rightToLeftValueMapping = rightToLeftValueMapping;
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

    @Override
    public String toString() {
        return "Match: (" + df.format(similarity) + ") " + leftTuple.toString() + " <-> " + rightTuple.toString() +
                (this.leftToRightValueMapping != null && !this.leftToRightValueMapping.isEmpty() ? "\nLeft to right value mapping:" + leftToRightValueMapping : "") +
                (this.rightToLeftValueMapping != null &&!this.rightToLeftValueMapping.isEmpty() ? "\nRight to left value mapping:" + rightToLeftValueMapping : "");
    }
}
