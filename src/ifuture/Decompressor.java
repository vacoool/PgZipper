package ifuture;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;

public class Decompressor extends BaseCompressor {
    public Decompressor(int threadCount) {
        super(threadCount);
    }

    public Decompressor() {
        super();
    }

    @Override
    protected IndexedBlock doTransform(IndexedBlock source) throws IOException {
        try (var in = new GZIPInputStream(new ByteArrayInputStream(source.getData()))) {
            var buffer = new byte[BLOCK_SIZE];
            var count = in.readNBytes(buffer, 0, buffer.length);
            in.close();
            return new IndexedBlock(source.getIndex(), buffer, count);
        }
    }

    @Override
    protected IndexedBlock doRead(InputStream stream, int index) throws IOException {

        if (stream.available() == 0)
            return null;
        final var offset = DATE_OFFSET + 4;

        var tmp = stream.readNBytes(offset);
        final var count = ByteBuffer.wrap(tmp, DATE_OFFSET, 4).getInt();
        if (count < 0 || count > MAX_COMPRESSED_BLOCK_SIZE)
            throw new IOException("Invalid input data");
        final var byteCount = count - offset;


        var buffer = new byte[count];
        System.arraycopy(tmp, 0, buffer, 0, offset);


        if (stream.read(buffer, offset, byteCount) != byteCount)
            throw new IOException("Damaged archive");
        return new IndexedBlock(index, buffer, count);
    }
}
