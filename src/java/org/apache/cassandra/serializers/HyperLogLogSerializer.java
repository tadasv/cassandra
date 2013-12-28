package org.apache.cassandra.serializers;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HyperLogLog;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tadas on 12/11/13.
 */
public class HyperLogLogSerializer<T> implements TypeSerializer<HyperLogLog<T>> {

    private static final Map<TypeSerializer<?>, HyperLogLogSerializer> instances = new HashMap<TypeSerializer<?>, HyperLogLogSerializer>();
    public final TypeSerializer<T> elements;

    public static synchronized <T> HyperLogLogSerializer<T> getInstance(TypeSerializer<T> elements)
    {
        HyperLogLogSerializer<T> t = instances.get(elements);
        if (t == null)
        {
            t = new HyperLogLogSerializer<T>(elements);
            instances.put(elements, t);
        }
        return t;
    }

    private HyperLogLogSerializer(TypeSerializer<T> elements)
    {
        this.elements = elements;
    }

    @Override
    public ByteBuffer serialize(HyperLogLog value) {
        return null;
    }

    @Override
    public HyperLogLog<T> deserialize(ByteBuffer bytes) {
        return null;
    }

    @Override
    public Class<HyperLogLog<T>> getType() {
        return null;
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException {
        return;
    }

    @Override
    public String toString(HyperLogLog value) {
        return "hyperloglog string representation";
    }
}
