package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram {

    // `frequencies[i]` contains the number of elements in the half-open interval
    // from `minValue + i * bucketWidth` to `minValue + (i + 1) * bucketWidth`.
    int minValue;
    int maxValue;
    int bucketWidth;
    int[] frequencies;
    int totalNumElements;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into `numBuckets` buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param numBuckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int numBuckets, int min, int max) {
        this.minValue = min;
        this.maxValue = max;

        // calculate the minimum required bucket width
        int totalWidth = max - min + 1; // + 1 because the max can be included
        this.bucketWidth = totalWidth / numBuckets;
        // round up if necessary
        if (totalWidth % numBuckets != 0) {
            this.bucketWidth += 1;
        }

        this.frequencies = new int[numBuckets];
        this.totalNumElements = 0;
    }

    private int getBucketIndex(int value) {
        assert value >= this.minValue;
        int bucketIndex = (value - this.minValue) / this.bucketWidth;
        assert bucketIndex < this.frequencies.length;
        return bucketIndex;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        this.frequencies[getBucketIndex(v)] += 1;
        this.totalNumElements += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op the operator used to compare the predicate argument to `v`
     * @param v the value on the right-hand side of the predicate expression
     * @return the predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        switch (op) {
            case EQUALS:
                return getFracElementsEqualTo(v);
            case NOT_EQUALS:
                return 1.0 - getFracElementsEqualTo(v);
            case LESS_THAN:
                return getFracElementsLessThan(v, false);
            case LESS_THAN_OR_EQ:
                return getFracElementsLessThan(v, true);
            case GREATER_THAN:
                return 1.0 - getFracElementsLessThan(v, true);
            case GREATER_THAN_OR_EQ:
                return 1.0 - getFracElementsLessThan(v, false);
            default:
                throw new UnsupportedOperationException(op + " is not a valid predicate operator for an IntHistogram");
        }
    }

    private double getFracElementsEqualTo(int value) {
        if (value < this.minValue || value > this.maxValue) {
            return 0.0;
        }

        int bucketIndex = this.getBucketIndex(value);
        return (double) (this.frequencies[bucketIndex] / this.bucketWidth) / this.totalNumElements;
    }

    private double getFracElementsLessThan(int value, boolean includeEqualTo) {
        if (value < this.minValue) {
            return 0.0;
        }
        if (value > this.maxValue) {
            return 1.0;
        }

        int bucketIndex = this.getBucketIndex(value);

        // calculate the fraction inside the bucket
        int leftEnd = this.minValue + bucketIndex * this.bucketWidth;
        int distanceFromLeftEnd = value - leftEnd + (includeEqualTo ? 1 : 0);
        double fractionOfBucket = (double) distanceFromLeftEnd / this.bucketWidth;
        double fractionInBucketOfTotal = fractionOfBucket * this.frequencies[bucketIndex] / this.totalNumElements;

        // calculate the number of entries to the left of the bucket
        int sum = 0;
        for (int i = 0; i < bucketIndex; ++i) {
            sum += this.frequencies[i];
        }

        double answer = fractionInBucketOfTotal + (double) sum / this.totalNumElements;
        return answer;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        return 1.0; // TODO what the heck is this
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
