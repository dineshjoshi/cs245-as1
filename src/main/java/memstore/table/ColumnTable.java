package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.List;

/**
 * ColumnTable, which stores data in column-major format.
 * That is, data is laid out like
 *   col 1 | col 2 | ... | col m.
 */
public class ColumnTable implements Table {
    int numCols;
    int numRows;
    ByteBuffer columns;
    IntBuffer view;

    public ColumnTable() { }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.columns = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        this.view = columns.asIntBuffer();

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ((colId * numRows) + rowId);
                this.view.put(offset, curRow.getInt(ByteFormat.FIELD_LEN*colId));
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = ((colId * numRows) + rowId);
        return view.get(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int offset = ((colId * numRows) + rowId);
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
        for (int r = 0; r < numRows; r++) {
            sum += view.get(r);
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

        for (int r = 0; r < numRows; r++) {
            final int c0v = view.get(offset(r, 0));

            if (c0v < threshold) {
                final int c2v = view.get(offset(r, 2));
                final int c3v = view.get(offset(r, 3));
                this.putIntField(r, 3, c2v + c3v);
                numRowsUpdated++;
            }
        }

        return numRowsUpdated;
    }

    private int offset(int r, int c)
    {
        return (c * numRows) + r;
    }
}
