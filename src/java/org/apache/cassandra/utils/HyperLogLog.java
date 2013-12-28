package org.apache.cassandra.utils;

import java.nio.ByteBuffer;

/**
 * Created by tadas on 12/11/13.
 */
public class HyperLogLog<T> {
    int precision;
    byte[] bitmap;

    public HyperLogLog(int precision)
    {
        this.precision = precision;
        this.bitmap = new byte[12];
        resetBitmap();
    }

    public ByteBuffer getBitmap()
    {
        return ByteBuffer.wrap(this.bitmap);
    }

    public void resetBitmap()
    {
        for (int i = 0; i < this.bitmap.length; i++) {
            this.bitmap[i] = 0;
        }
    }

    public void setBitmap(ByteBuffer bm)
    {
        byte [] bmarray = bm.array();
        assert bmarray.length == this.bitmap.length;
        for (int i = 0; i < bmarray.length; i++) {
            this.bitmap[i] = bmarray[i];
        }
    }

    public void add(ByteBuffer item)
    {
        this.bitmap[11]++;
    }
}
