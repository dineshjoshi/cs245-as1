package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 * Custom table implementation to adapt to provided query mix.
 */
public class CustomTable extends ColumnTable {
    long[] idx;

    public CustomTable() { }

    @Override
    public void load(DataLoader loader) throws IOException {
        super.load(loader);
        idx = new long[numRows * numCols];
        rebuildIndex();
    }

    @Override
    public void putIntField(int rowId, int colId, int field) {
        final int offset = ((colId * numRows) + rowId);
        final int oldVal = view.get(offset);
        idx[rowId] += (field - oldVal);
        view.put(offset, field);
    }

    @Override
    public long predicatedAllColumnsSum(int threshold) {
        long sum = 0;

        for (int r = 0; r < numRows; r++) {
            final int c0v = view.get(offset(r, 0));
            if (c0v > threshold)
                sum += idx[r];
        }

        return sum;
    }

    private void rebuildIndex() {
        for (int r = 0; r < numRows; r++) {
            long sum = 0;
            for (int c = 0; c < numCols; c++) {
                final int val = view.get(offset(r, c));
                sum += val;
            }
            idx[r] = sum;
        }
    }
}
