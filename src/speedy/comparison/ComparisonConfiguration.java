package speedy.comparison;

import speedy.SpeedyConstants;

public class ComparisonConfiguration {

    private static String[] stringSkolemPrefixes = {SpeedyConstants.SKOLEM_PREFIX, "_N"};

    private static String[] numericSkolemPrefixes = {
        SpeedyConstants.BIGINT_SKOLEM_PREFIX,
        SpeedyConstants.REAL_SKOLEM_PREFIX};

    private static String[] stringLlunPrefixes = {SpeedyConstants.LLUN_PREFIX};

    private static String[] numericLlunPrefixes = {
        SpeedyConstants.BIGINT_LLUN_PREFIX,
        SpeedyConstants.REAL_LLUN_PREFIX};

    private static boolean twoWayValueMapping = true;
    private static boolean injective = false;
    private static boolean stopIfNonMatchingTuples = false;
    private static double K = 0.5;
    private static boolean convertSkolemInHash = false;

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

    public static String[] getStringSkolemPrefixes() {
        return stringSkolemPrefixes;
    }

    public static void setStringSkolemPrefixes(String[] stringSkolemPrefixes) {
        ComparisonConfiguration.stringSkolemPrefixes = stringSkolemPrefixes;
    }

    public static String[] getStringLlunPrefixes() {
        return stringLlunPrefixes;
    }

    public static void setStringLlunPrefixes(String[] stringLlunPrefixes) {
        ComparisonConfiguration.stringLlunPrefixes = stringLlunPrefixes;
    }

    public static String[] getNumericSkolemPrefixes() {
        return numericSkolemPrefixes;
    }

    public static void setNumericSkolemPrefixes(String[] numericSkolemPrefixes) {
        ComparisonConfiguration.numericSkolemPrefixes = numericSkolemPrefixes;
    }

    public static String[] getNumericLlunPrefixes() {
        return numericLlunPrefixes;
    }

    public static void setNumericLlunPrefixes(String[] numericLlunPrefixes) {
        ComparisonConfiguration.numericLlunPrefixes = numericLlunPrefixes;
    }

    public static boolean isStopIfNonMatchingTuples() {
        return stopIfNonMatchingTuples;
    }

    public static void setStopIfNonMatchingTuples(boolean stopIfNonMatchingTuples) {
        ComparisonConfiguration.stopIfNonMatchingTuples = stopIfNonMatchingTuples;
    }

    public static boolean isConvertSkolemInHash() {
        return convertSkolemInHash;
    }

    public static void setConvertSkolemInHash(boolean convertSkolemInHash) {
        ComparisonConfiguration.convertSkolemInHash = convertSkolemInHash;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("nullPrefixes:").append(stringSkolemPrefixes).append("\n");
        sb.append("twoWayValueMapping:").append(twoWayValueMapping).append("\n");
        sb.append("injective:").append(injective).append("\n");
        sb.append("stopIfNonMatchingTuples:").append(stopIfNonMatchingTuples).append("\n");
        sb.append("K:").append(K).append("\n");
        sb.append("convertSkolemInHash:").append(convertSkolemInHash).append("\n");
        return sb.toString();
    }

}
