package speedy.comparison;

import java.text.DecimalFormat;
import speedy.model.database.Tuple;

public class TupleMatch {

    private final static DecimalFormat df = new DecimalFormat("#0.00");
    private Tuple expected;
    private Tuple generated;
    private double similarity;

    public TupleMatch(Tuple expected, Tuple generated, double similarity) {
        this.expected = expected;
        this.generated = generated;
        this.similarity = similarity;
    }

    public Tuple getExpected() {
        return expected;
    }

    public Tuple getGenerated() {
        return generated;
    }

    public double getSimilarity() {
        return similarity;
    }

    @Override
    public String toString() {
        return "Match: (" + df.format(similarity) + ") " + expected.toString() + " <-> " + generated.toString();
    }
}
