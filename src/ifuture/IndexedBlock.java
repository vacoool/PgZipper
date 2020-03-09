package ifuture;

//Simple DTO between queues

public class IndexedBlock {
    private final int index;
    private final byte[] data;
    private final int length;

    public IndexedBlock(int index, byte[] data, int length) {
        if (data == null)
            throw new IllegalArgumentException();
        this.index = index;
        this.data = data;
        this.length = length;
    }

    public int getIndex() {
        return index;
    }

    public byte[] getData() {
        return data;
    }

    public int getLength() {
        return length;
    }
}
