package org.apache.spark.app.flint;

import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by zx on 16-11-14.
 */
public class SlowGrowCompressByteStream extends SlowGrowByteStream {

    public SlowGrowCompressByteStream() {this(32);}

    public SlowGrowCompressByteStream(int size){
        super(size);
    }

    @Override
    protected void grow(int minCapacity) {
        // overflow-conscious code
        int oldCapacity = buf.length;
        int newCapacity = 0;
        if(oldCapacity < 20971520)
            newCapacity = oldCapacity << 1;
        else
            newCapacity = oldCapacity + 5242880;
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity < 0) {
            if (minCapacity < 0) // overflow
                throw new OutOfMemoryError();
            newCapacity = Integer.MAX_VALUE;
        }
        buf = Arrays.copyOf(buf, newCapacity);
    }

    public void compress() throws IOException {
        byte[] compressedStream = Snappy.compress(buf);
        buf = compressedStream;
    }

    public byte[] decompress() throws IOException {
        return Snappy.uncompress(buf);
    }

}
