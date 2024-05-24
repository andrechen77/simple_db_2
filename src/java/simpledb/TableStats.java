package simpledb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 *
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    int tableId;
    int ioCostPerPage;
    TupleDesc tupleDesc;
    int numPages;
    int numTuples;
    Histogram[] histograms;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        try {
            this.tableId = tableid;
            this.ioCostPerPage = ioCostPerPage;

            HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
            this.numPages = heapFile.numPages();

            this.tupleDesc = heapFile.getTupleDesc();

            // scan through once to find the min and max of each field
            int[] minimums = new int[tupleDesc.numFields()];
            Arrays.fill(minimums, Integer.MAX_VALUE);
            int[] maximums = new int[tupleDesc.numFields()];
            Arrays.fill(maximums, Integer.MIN_VALUE);
            SeqScan seqScan = new SeqScan(new TransactionId(), tableid);
            seqScan.open();
            while (seqScan.hasNext()) {
                Tuple tuple = seqScan.next();
                for (int f = 0; f < tupleDesc.numFields(); ++f) {
                    if (tupleDesc.getFieldType(f) == Type.INT_TYPE) {
                        int value = ((IntField) tuple.getField(f)).getValue();
                        minimums[f] = Math.min(minimums[f], value);
                        maximums[f] = Math.max(maximums[f], value);
                    }
                }
            }

            // create the histograms to fill
            this.histograms = new Histogram[tupleDesc.numFields()];
            int numBuckets = 10;
            for (int f = 0; f < tupleDesc.numFields(); ++f) {
                switch (tupleDesc.getFieldType(f)) {
                    case INT_TYPE:
                        this.histograms[f] = new IntHistogram(numBuckets, minimums[f], maximums[f]);
                        break;
                    case STRING_TYPE:
                        this.histograms[f] = new StringHistogram(numBuckets);
                        break;
                }
            }

            // scan through the entire page to fill in the histograms
            this.numTuples = 0;
            seqScan.rewind();
            while (seqScan.hasNext()) {
                Tuple tuple = seqScan.next();
                this.numTuples += 1;
                for (int f = 0; f < tupleDesc.numFields(); ++f) {
                    switch (tupleDesc.getFieldType(f)) {
                        case INT_TYPE:
                            ((IntHistogram) this.histograms[f]).addValue(((IntField) tuple.getField(f)).getValue());
                            break;
                        case STRING_TYPE:
                            ((StringHistogram) this.histograms[f]).addValue(((StringField) tuple.getField(f)).getValue());
                            break;
                    }
                }
            }

            System.err.println("numTuples: " + numTuples);
        } catch (DbException e) {
            throw new RuntimeException(e);
        } catch (TransactionAbortedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     *
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return this.numPages * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (this.numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        return this.histograms[field].avgSelectivity();
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        switch (this.tupleDesc.getFieldType(field)) {
            case INT_TYPE:
                int intValue = ((IntField) constant).getValue();
                return ((IntHistogram) this.histograms[field]).estimateSelectivity(op, intValue);
            case STRING_TYPE:
                String stringValue = ((StringField) constant).getValue();
                return ((StringHistogram) this.histograms[field]).estimateSelectivity(op, stringValue);
            default:
                throw new RuntimeException("unreachable");
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return this.numTuples;
    }
}
