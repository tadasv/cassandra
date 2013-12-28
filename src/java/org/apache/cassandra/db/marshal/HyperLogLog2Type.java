package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.serializers.BytesSerializer;
import org.apache.cassandra.serializers.TypeSerializer;


public class HyperLogLog2Type extends AbstractType<ByteBuffer> {
    public static final HyperLogLog2Type instance = new HyperLogLog2Type();

    HyperLogLog2Type() {} // singleton

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        return BytesType.bytesCompare(o1, o2);
    }
/*
    public static int bytesCompare(ByteBuffer o1, ByteBuffer o2)
    {
        if (o1 == null)
            return o2 == null ? 0 : -1;

        return ByteBufferUtil.compareUnsigned(o1, o2);
    }
*/
    public ByteBuffer fromString(String source)
    {
        return ByteBuffer.wrap(source.getBytes());
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        // Both asciiType and utf8Type really use bytes comparison and
        // bytesType validate everything, so it is compatible with the former.
        return this == previous || previous == AsciiType.instance || previous == UTF8Type.instance;
    }

    @Override
    public boolean isValueCompatibleWith(AbstractType<?> previous)
    {
        // BytesType can read anything
        return true;
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.HYPERLOGLOG;
    }

    public TypeSerializer<ByteBuffer> getSerializer()
    {
        return BytesSerializer.instance;
    }
}
