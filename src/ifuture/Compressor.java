package ifuture;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPOutputStream;

public class Compressor extends BaseCompressor {
    public Compressor(int threadCount) {
        super(threadCount);
    }
    public Compressor() {
        super();
    }

    private static void WriteLength(byte[] buffer, int length) {
        ByteBuffer.wrap(buffer, DATE_OFFSET, 4).putInt(length);
    }

    @Override
    protected IndexedBlock doTransform(IndexedBlock source) throws IOException {
        try (var ts = new FixedBufferOutputStream(MAX_COMPRESSED_BLOCK_SIZE);
             var out = new GZIPOutputStream(ts)) {
            out.write(source.getData(), 0, source.getLength());
            out.finish();
            WriteLength(ts.buffer(), ts.size());
            return new IndexedBlock(source.getIndex(), ts.buffer(), ts.size());
        }
    }

    @Override
    protected IndexedBlock doRead(InputStream stream, final int index) throws IOException {
        var buffer = new byte[BLOCK_SIZE];
        var count = stream.read(buffer, 0, buffer.length);
        if (count <= 0) return null;
        return new IndexedBlock(index, buffer, count);
    }
}
