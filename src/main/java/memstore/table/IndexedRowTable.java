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
    private TreeMap<Integer, IntArrayList> index;

    private Index[] indices;
    private ByteBuffer rows;
    private int indexColumn;
    private IntBuffer view;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
        this.index = new TreeMap<>();
        this.indices = new Index[3];
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
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.indices[0] = new Index(numRows, numCols);
        this.indices[1] = new Index(numRows, numCols);
        this.indices[2] = new Index(numRows, numCols);

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
        final int offset = offset(rowId, colId);

        if (colId == indexColumn) {
            final int oldVal = view.get(offset);
            indexFor(0).update(rowId, oldVal, field);
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

        for (int r = 0; r < numRows; r++) {
            final int c0v = view.get(offset(r, 0));
            final int c1v = view.get(offset(r, 1));
            final int c2v = view.get(offset(r, 2));
            sum += (c1v > threshold1 && c2v < threshold2) ? c0v : 0;
        }

        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        long sum = 0;

        Set<Integer> rowsToModify = indexFor(0).gt(threshold);

        for (int r : rowsToModify) {
            for (int c = 0; c < numCols; c++) {
                final int val = view.get(offset(r, c));
                sum += val;
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

        Set<Integer> rowsToModify = indexFor(0).lt(threshold);

        for (int r : rowsToModify) {
            final int c2v = view.get(offset(r, 2));
            final int c3v = view.get(offset(r, 3));
            this.putIntField(r, 3, c2v + c3v);
            numRowsUpdated++;
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
//    public void printIndex() {
//        System.out.println("\nIndex\n");
//        for (Map.Entry<Integer, IntArrayList> e: index.entrySet()) {
//            System.out.printf("%d -> %s\n", e.getKey(), Arrays.toString(e.getValue().toIntArray()));
//        }
//    }

    static class Index {
        private final TreeMap<Integer, IntArrayList> idx = new TreeMap<>();
        final int numRows;
        final int numCols;

        public Index(int numRows, int numCols) {
            this.numRows = numRows;
            this.numCols = numRows;
        }

        private void update(int rowId, int oldVal, int newVal) {
            IntArrayList newValIdx = idx.getOrDefault(newVal, new IntArrayList());
            IntArrayList oldValIdx = idx.getOrDefault(oldVal, new IntArrayList());
            oldValIdx.rem(rowId);
            newValIdx.push(rowId);
            idx.put(oldVal, oldValIdx);
            idx.put(newVal, newValIdx);
        }
        
        private Set<Integer> gt(int t) {
            SortedSet<Integer> candidateVals = idx.navigableKeySet().tailSet(t, false);
            Set<Integer> rowsToModify = new HashSet<>(numRows);

            for (Integer c : candidateVals) {
                IntArrayList temp = idx.get(c);
                rowsToModify.addAll(temp);
            }
            return rowsToModify;
        }


        private Set<Integer> lt(int t) {
            SortedSet<Integer> candidateVals = idx.navigableKeySet().headSet(t);
            Set<Integer> rowsToModify = new HashSet<>(numRows);

            for (Integer c : candidateVals) {
                IntArrayList temp = idx.get(c);
                rowsToModify.addAll(temp);
            }
            return rowsToModify;
        }
    }

    private Index indexFor(int c) {
        return indices[c];
    }
}
