package ifuture;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.*;

public abstract class BaseCompressor {

    private static final IndexedBlock DUMMY_BLOCK = new IndexedBlock(-1, new byte[0], 0);
    private static final int FIRST_BLOCK_INDEX = 0;

    protected static final int DATE_OFFSET = 4;
    protected static final int BLOCK_SIZE = 0x10000; //0x10000 (65KB) gives quite good [memory usage]/[performance] ratio
    protected static final int MAX_COMPRESSED_BLOCK_SIZE = BLOCK_SIZE + 0x100;

    private final LinkedBlockingQueue<IndexedBlock> transformQueue; // read -> transform
    private final LinkedBlockingQueue<IndexedBlock> writeQueue; // transform -> write
    private final int threadCount;
    private volatile int blockIndex;

    protected BaseCompressor(int threadCount) {
        if (threadCount < 1)
            throw new IllegalArgumentException("threadCount must be greater than 1");
        this.threadCount = threadCount;
        final var queueLength = 4 * threadCount;
        transformQueue = new LinkedBlockingQueue<>(queueLength);
        writeQueue = new LinkedBlockingQueue<>(queueLength);
    }

    protected BaseCompressor() {
        this(Runtime.getRuntime().availableProcessors());
    }

    public long getProgress() {
        return blockIndex * (long)BLOCK_SIZE;
    }

    public void process(String input, String output) throws IOException, ProcessException {
        transformQueue.clear();
        writeQueue.clear();
        blockIndex = FIRST_BLOCK_INDEX;

        final var barrier = new CyclicBarrier(threadCount, () -> {
            try {
                writeQueue.put(DUMMY_BLOCK);
            } catch (InterruptedException e) {
                //no need to handle exception, shutdownNow will be called by executor exception handler
            }
        });

        try (final var outStream = new FileOutputStream(output);
             final var inStream = new FileInputStream(input)) {

            final var jobs = new ArrayList<Callable<Boolean>>(Collections.nCopies(threadCount, () -> transform(barrier)));
            jobs.add(0, () -> Read(inStream));
            jobs.add(() -> write(outStream));

            final var executor = Executors.newFixedThreadPool(jobs.size());
            final var service = new ExecutorCompletionService<Boolean>(executor);

            for (var job : jobs) {
                service.submit(job);
            }

            try {
                while (!service.take().get()) ;
            } catch (InterruptedException | ExecutionException e) {
                throw new ProcessException("Internal error", e.getCause());
            } finally {
                executor.shutdownNow();
            }
        }
    }

    protected abstract IndexedBlock doRead(InputStream stream, int index) throws IOException;

    protected abstract IndexedBlock doTransform(IndexedBlock source) throws IOException;

    private boolean Read(InputStream stream) throws IOException, InterruptedException {
        IndexedBlock block;

        //no problem with non-atomic increment, reading is done in single thread
        while ((block = doRead(stream, blockIndex++)) != null) {
            transformQueue.put(block);
        }

        for (var dummy : Collections.nCopies(threadCount, DUMMY_BLOCK)) {
            transformQueue.put(dummy);
        }

        return false; //return value to mark last job in pipeline
    }

    private boolean transform(CyclicBarrier barrier) throws IOException, BrokenBarrierException, InterruptedException {
        IndexedBlock source;
        while ((source = transformQueue.take()) != DUMMY_BLOCK) {
            var target = doTransform(source);
            writeQueue.put(target);
        }
        barrier.await();
        return false; //return value to mark last job in pipeline
    }

    private boolean write(OutputStream stream) throws Exception {
        //if one thread works slower than others or compression of some block takes longer, local queue may temporarily grow
        //if we need fixed upper bound for memory consumption, Barrier inside loop in Transform method is needed
        var localBuffer = new BlockBuffer(FIRST_BLOCK_INDEX);
        IndexedBlock block;
        while ((block = writeQueue.take()) != DUMMY_BLOCK) {
            localBuffer.insert(block);
            while ((block = localBuffer.take()) != null) {
                stream.write(block.getData(), 0, block.getLength());
            }
        }
        return true; //return value to mark last job in pipeline

    }

    //Circular buffer for Blocks
    //We accumulate Blocks until next to be written block is inserted
    //To keep buffer sorted we insert each new block at position, calculated by following rule:
    //[head] + [index of to be inserted block] - [index of next to be written Block (currentIndex)]
    private static class BlockBuffer {
        private int currentIndex; //following index in sequence of indexes (next to be written block)
        private IndexedBlock[] data = new IndexedBlock[0x40];
        private int head; //offset (position of next to be written block)

        public BlockBuffer(int firstBlockIndex) {
            currentIndex = firstBlockIndex;
        }

        /// <exception cref="T:System.ArgumentOutOfRangeException">Index order is broken.</exception>
        public void insert(IndexedBlock value) {
            if (value.getIndex() < currentIndex) {
                throw new IllegalArgumentException("Index order is broken.");
            }

            var offset = value.getIndex() - currentIndex;
            //grow
            if (offset >= data.length)
                resize();

            data[getAbsIndex(offset)] = value;
        }

        public IndexedBlock take() {
            // We can Pop Block only if its index is next in series
            var value = data[head];
            if (value != null && value.getIndex() == currentIndex) {
                data[head] = null;
                head = getAbsIndex(1);
                ++currentIndex;
                return value;
            }

            return null;
        }

        private void resize() {
            var tmp = new IndexedBlock[data.length * 2]; //array must initially have Length equal to at least 1
            System.arraycopy(data, head, tmp, 0, data.length - head);
            System.arraycopy(data, 0, tmp, data.length - head, head);
            data = tmp;
            head = 0;
        }

        private int getAbsIndex(int delta) {
            var result = head + delta;
            return result < data.length ? result : result - data.length;
        }
    }
}
