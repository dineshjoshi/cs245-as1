package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.*;

/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {
    int numCols;
    int numRows;

    private IndexIntArrayList[] indices;
    private ByteBuffer rows;
    private int indexColumn;
    private IntBuffer view;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        this.indices = new IndexIntArrayList[numCols];
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();

        indices[0] = new IndexIntArrayList(numRows, numCols);
        indices[2] = new IndexIntArrayList(numRows, numCols);
        if (indexColumn != 0 && indexColumn != 2)
            indices[indexColumn] = new IndexIntArrayList(numRows, numCols);

        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        this.view = this.rows.asIntBuffer();

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                this.putIntField(rowId, colId, curRow.getInt(ByteFormat.FIELD_LEN * colId));
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        return view.get(offset(rowId, colId));
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        putIntField(rowId, colId, field, true);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    private void putIntField(int rowId, int colId, int field, boolean updateIndex) {
        final int offset = offset(rowId, colId);

        if (updateIndex && (colId == 0 || colId == 2 || colId == indexColumn)) {
            final int oldVal = view.get(offset);
            indexFor(colId).update(rowId, oldVal, field);
        }

        view.put(offset, field);
    }


    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        long sum = 0;

        for (int r=0; r < numRows; r++) {
            sum += view.get(offset(r, 0));
        }

        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        long sum = 0;
        IndexIntArrayList idx = indexFor(2);
        final SortedMap<Integer, IntArrayList> rowSet = idx.lt2(threshold2);

        for (IntArrayList row : rowSet.values()) {
            for (int r : row) {
                final int off = offset(r, 0);
                final int c1v = view.get(off + 1);
                if (c1v > threshold1)
                    sum += view.get(off);
            }
        }

        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
//    @Override
//    public long predicatedAllColumnsSum(int threshold) {
//        long sum = 0;
//
//        final IndexIntArrayList idx = indexFor(0);
////        final NavigableMap<Integer, IntArrayList> rowSet = idx.gt2(threshold);
//        final Set<IntArrayList> rowSet = idx.gt(threshold);
//
////        for (IntArrayList row : rowSet.values()) {
//            for (IntArrayList row : rowSet) {
//                for (int r : row) {
//                    for (int c = 0; c < numCols; c++) {
//                        final int val = view.get(offset(r, c));
//                        sum += val;
//                    }
//                }
//            }
////        }
//
////        for (int r = 0; r < numRows; r++) {
////            final int c0v = view.get(offset(r, 0));
////
////            if (c0v > threshold) {
////                sum += c0v;
////                for (int c = 1; c < numCols; c++) {
////                    final int val = view.get(offset(r, c));
////                    sum += val;
////                }
////            }
////        }
//
//        return sum;
//    }
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        long sum = 0;

        for (int r = 0; r < numRows; r++) {
            final int c0v = view.get(offset(r, 0));

            if (c0v > threshold) {
                sum += c0v;
                for (int c = 1; c < numCols; c++) {
                    final int val = view.get(offset(r, c));
                    sum += val;
                }
            }
        }

        return sum;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        int numRowsUpdated = 0;

        final IndexIntArrayList idx = indexFor(0);
        SortedMap<Integer, IntArrayList> rowSet = idx.lt2(threshold);

        for (IntArrayList row : rowSet.values()) {
            for (int r : row) {
                final int offset = offset(r, 2);
                final int c2v = view.get(offset);
                final int c3v = view.get(offset + 1);
                this.view.put(offset + 1, c2v + c3v);
                numRowsUpdated++;
            }
        }

        return numRowsUpdated;
    }

    private int offset(int r, int c) {
        return (r * numCols) + c;
    }

//    public void printTable() {
//        System.out.println("\nTable\n");
//
//        for (int r = 0; r < numRows; r++) {
//            System.out.printf("%02d. ", r);
//            for (int c = 0; c < numCols; c++) {
//                System.out.printf("%d\t", getIntField(r, c));
//            }
//            System.out.println("");
//        }
//    }
//
    public void printIndex() {
        indexFor(0).printIndex();
    }

    private IndexIntArrayList indexFor(int c) {
        return indices[c];
    }


    /*--------- INDEX ----------*/

    interface Index {
        void update(int rowId, int oldVal, int newVal);
        Set<IntArrayList> gt(int t);
        Set<IntArrayList> lt(int t);

        SortedMap<Integer, IntArrayList> lt2(int t);
        NavigableMap<Integer, IntArrayList> gt2(int t);

        IntArrayList get(int key);
        void printIndex();
        void rebuild();

    }

    static class IndexIntArrayList implements Index {
        private final TreeMap<Integer, IntArrayList> idx = new TreeMap<>();
        final int numRows;
        final int numCols;

        public IndexIntArrayList(int numRows, int numCols) {
            this.numRows = numRows;
            this.numCols = numCols;
        }

        public void update(int rowId, int oldVal, int newVal) {
            IntArrayList newValIdx = idx.getOrDefault(newVal, new IntArrayList());
            IntArrayList oldValIdx = idx.getOrDefault(oldVal, new IntArrayList());
            oldValIdx.rem(rowId);
            newValIdx.push(rowId);
            idx.put(oldVal, oldValIdx);
            idx.put(newVal, newValIdx);
        }
        
        public Set<IntArrayList> gt(int t) {
            Set<Integer> candidates = idx.navigableKeySet().tailSet(t, false);
            Set<IntArrayList> res = new HashSet<>();

            for (Integer c : candidates)
                res.add(idx.get(c));

            return res;
        }

        public Set<IntArrayList> lt(int t) {
            Set<Integer> candidates = idx.navigableKeySet().headSet(t);
            Set<IntArrayList> res = new HashSet<>();

            for (Integer c : candidates)
                res.add(idx.get(c));

            return res;
        }

        public SortedMap<Integer, IntArrayList> lt2(int t) {
            return idx.headMap(t);
        }

        public NavigableMap<Integer, IntArrayList> gt2(int t) {
            return idx.tailMap(t, false);
        }

        public IntArrayList get(int key) {
            return idx.get(key);
        }

        public void printIndex() {
            System.out.println("\nIndex\n");
            for (Map.Entry<Integer, IntArrayList> e : idx.entrySet()) {
                System.out.printf("%d -> %s\n", e.getKey(), Arrays.toString(e.getValue().toIntArray()));
            }
        }

        @Override
        public void rebuild() {
            // NoOp for now
        }
    }
}
