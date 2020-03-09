package ifuture;

import java.io.IOException;
import java.io.OutputStream;

//simple fixed size array wrapper to reuse buffer without making a copy
public class FixedBufferOutputStream extends OutputStream {

    private final byte[] buffer;
    private int pos = -1;
    boolean closed;

    public FixedBufferOutputStream(final int capacity) {

        this.buffer = new byte[capacity];
    }

    public byte[] buffer() {
        return buffer;
    }

    public int size() {
        return pos + 1;
    }


    @Override
    public void write(final int b) throws IOException {
        if (closed)
            throw new IOException("Stream is closed");
        if (++pos == buffer.length)
            throw new IOException("Buffer overflow");
        buffer[pos] = (byte)b;
    }


    @Override
    public void write(final byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        if (closed)
            throw new IOException("Stream is closed");
        System.arraycopy(b, off, buffer, pos + 1, len);
        pos += len;
    }

    @Override
    public void close() {
        closed = true;
    }
}