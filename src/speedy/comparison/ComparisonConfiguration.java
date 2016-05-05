package speedy.comparison;

import static speedy.SpeedyConstants.SKOLEM_PREFIX;

public class ComparisonConfiguration {

    private static String[] nullPrefixes = {SKOLEM_PREFIX, "_N"};
    private static boolean twoWayValueMapping = true;
    private static boolean injective = false;
    private static boolean stopIfNonMatchingTuples = false;
    private static double K = 0.5;

    public static boolean isTwoWayValueMapping() {
        return twoWayValueMapping;
    }

    public static void setTwoWayValueMapping(boolean twoWayValueMapping) {
        ComparisonConfiguration.twoWayValueMapping = twoWayValueMapping;
    }

    public static boolean isInjective() {
        return injective;
    }

    public static void setInjective(boolean injective) {
        ComparisonConfiguration.injective = injective;
    }

    public static double getK() {
        return K;
    }

    public static void setK(double K) {
        ComparisonConfiguration.K = K;
    }

    public static String[] getNullPrefixes() {
        return nullPrefixes;
    }

    public static void setNullPrefixes(String[] nullPrefixes) {
        ComparisonConfiguration.nullPrefixes = nullPrefixes;
    }

    public static boolean isStopIfNonMatchingTuples() {
        return stopIfNonMatchingTuples;
    }

    public static void setStopIfNonMatchingTuples(boolean stopIfNonMatchingTuples) {
        ComparisonConfiguration.stopIfNonMatchingTuples = stopIfNonMatchingTuples;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("nullPrefixes:").append(nullPrefixes).append("\n");
        sb.append("twoWayValueMapping:").append(twoWayValueMapping).append("\n");
        sb.append("injective:").append(injective).append("\n");
        sb.append("stopIfNonMatchingTuples:").append(stopIfNonMatchingTuples).append("\n");
        sb.append("K:").append(K).append("\n");
        return sb.toString();
    }
    
}
