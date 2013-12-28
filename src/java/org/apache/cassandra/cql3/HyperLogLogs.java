package org.apache.cassandra.cql3;

import com.google.common.base.Joiner;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.HyperLogLog;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.utils.Pair;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Static helper methods and classes for hyperloglogs.
 */
public abstract class HyperLogLogs
{
    private HyperLogLogs() {}

    public static ColumnSpecification valueSpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ")", true), ((HyperLogLogType)column.type).elements);
    }

    public static class Literal implements Term.Raw
    {
        private final List<Term.Raw> elements;

        public Literal(List<Term.Raw> elements)
        {
            this.elements = elements;
        }

        public Term prepare(ColumnSpecification receiver) throws InvalidRequestException
        {
            validateAssignableTo(receiver);

            ColumnSpecification valueSpec = HyperLogLogs.valueSpecOf(receiver);
            Set<Term> values = new HashSet<Term>(elements.size());
            boolean allTerminal = true;
            for (Term.Raw rt : elements)
            {
                Term t = rt.prepare(valueSpec);

                if (t.containsBindMarker())
                    throw new InvalidRequestException(String.format("Invalid set literal for %s: bind variables are not supported inside collection literals", receiver));

                if (t instanceof Term.NonTerminal)
                    allTerminal = false;

                values.add(t);
            }

            DelayedValue value = new DelayedValue(((HyperLogLogType)receiver.type).elements, values);

            return allTerminal ? value.bind(Collections.<ByteBuffer>emptyList()) : value;
        }

        private void validateAssignableTo(ColumnSpecification receiver) throws InvalidRequestException
        {
            ColumnSpecification valueSpec = HyperLogLogs.valueSpecOf(receiver);
            for (Term.Raw rt : elements)
            {
                if (!rt.isAssignableTo(valueSpec))
                    throw new InvalidRequestException(String.format("Invalid hyperloglog literal for %s: value %s is not of type %s", receiver, rt, valueSpec.type.asCQL3Type()));
            }
        }

        public boolean isAssignableTo(ColumnSpecification receiver)
        {
            try
            {
                validateAssignableTo(receiver);
                return true;
            }
            catch (InvalidRequestException e)
            {
                return false;
            }
        }

        @Override
        public String toString()
        {
            return "hyperloglog{" + Joiner.on(", ").join(elements) + "}";
        }
    }

    public static class Value extends Term.Terminal
    {
        public final Set<ByteBuffer> elements;

        public Value(Set<ByteBuffer> elements)
        {
            this.elements = elements;
        }

        public static Value fromSerialized(ByteBuffer value, HyperLogLogType type) throws InvalidRequestException
        {
            // when pulling data from disk(?)
            Set<ByteBuffer> elements = new LinkedHashSet<ByteBuffer>();
            elements.add(ByteBuffer.wrap(new String("fromSerialized").getBytes()));
            return new Value(elements);
            /*
            try
            {
                // Collections have this small hack that validate cannot be called on a serialized object,
                // but compose does the validation (so we're fine).
                Set<?> s = (Set<?>)type.compose(value);
                Set<ByteBuffer> elements = new LinkedHashSet<ByteBuffer>(s.size());
                for (Object element : s)
                    elements.add(type.elements.decompose(element));
                return new Value(elements);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
            */
        }

        public ByteBuffer get()
        {
            // Serialized HLL value, the actual bitmap.
            return ByteBuffer.wrap(new String("packed").getBytes());
            //return CollectionType.pack(new ArrayList<ByteBuffer>(elements), elements.size());
        }
    }

    public static class DelayedValue extends Term.NonTerminal
    {
        private final Comparator<ByteBuffer> comparator;
        private final Set<Term> elements;

        public DelayedValue(Comparator<ByteBuffer> comparator, Set<Term> elements)
        {
            this.comparator = comparator;
            this.elements = elements;
        }

        public boolean containsBindMarker()
        {
            // False since we don't support them in collection
            return false;
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
        }

        public Value bind(List<ByteBuffer> values) throws InvalidRequestException
        {
            Set<ByteBuffer> buffers = new TreeSet<ByteBuffer>(comparator);
            for (Term t : elements)
            {
                ByteBuffer bytes = t.bindAndGet(values);

                if (bytes == null)
                    throw new InvalidRequestException("null is not supported inside collections");

                // We don't support value > 64K because the serialization format encode the length as an unsigned short.
                if (bytes.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
                    throw new InvalidRequestException(String.format("Set value is too long. Set values are limited to %d bytes but %d bytes value provided",
                            FBUtilities.MAX_UNSIGNED_SHORT,
                            bytes.remaining()));

                buffers.add(bytes);
            }
            return new Value(buffers);
        }
    }

    public static class Setter extends Operation
    {
        public Setter(ColumnIdentifier column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, ColumnNameBuilder prefix, UpdateParameters params) throws InvalidRequestException
        {
            // delete + add
            ColumnNameBuilder column = prefix.add(columnName);
            cf.addAtom(params.makeTombstoneForOverwrite(column.build(), column.buildAsEndOfRange()));
            Adder.doAdd(rowKey, columnName, t, cf, column, params);
        }
    }

    public static class Adder extends Operation
    {
        @Override
        public boolean requiresRead() {
            return true;
        }

        public Adder(ColumnIdentifier column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, ColumnNameBuilder prefix, UpdateParameters params) throws InvalidRequestException
        {
            doAdd(rowKey, columnName, t, cf, prefix.add(columnName), params);
        }

        static void doAdd(ByteBuffer rowKey, ColumnIdentifier columnName, Term t, ColumnFamily cf, ColumnNameBuilder columnNameBuilder, UpdateParameters params) throws InvalidRequestException
        {
            Term.Terminal value = t.bind(params.variables);
            if (value == null)
                return;

            HyperLogLog<String> hll = new HyperLogLog<String>(5);
            Set<ByteBuffer> toAdd = ((Sets.Value)value).elements;
            for (ByteBuffer bb : toAdd) {
                hll.add(null);
            }

            ByteBuffer cellName = columnNameBuilder.copy().add(ByteBuffer.wrap(new String("hll_bitmap").getBytes())).build();
            cf.addColumn(params.makeColumn(
                            cellName,
                            hll.getBitmap()));

            return;

            /*
            ByteBuffer ddd = value.get();
            // We need to grab existing data from disk
            // add new items and write it out
            int componenetCount = columnNameBuilder.componentCount();
            int columnCount = cf.getColumnCount();
            for (ByteBuffer name : cf.getColumnNames()) {
                //Column col = cf.getColumn(name);
                System.out.println("sss");
            }
            List<Pair<ByteBuffer, Column>> existingList = params.getPrefetchedList(rowKey, columnName);

            HyperLogLog<String> hll;

            if (existingList.size() == 2) {
                // there should always be two items in the list
                // hll precision and hll_bitmap
            } else {
                assert existingList.size() == 0;
            }

            assert value instanceof Sets.Value : value;
            ByteBuffer cellName = columnNameBuilder.copy().add(ByteBuffer.wrap(new String("precision").getBytes())).build();
            cf.addColumn(params.makeColumn(
                    cellName,
                    ByteBuffer.wrap(new String("precision_value").getBytes())));

            cellName = columnNameBuilder.copy().add(ByteBuffer.wrap(new String("hll_bitmap").getBytes())).build();
            cf.addColumn(params.makeColumn(
                    cellName,
                    ByteBuffer.wrap(new String("HLL_BITMAP_DATA").getBytes())));
            /*
            Set<ByteBuffer> toAdd = ((HyperLogLogs.Value)value).elements;
            for (ByteBuffer bb : toAdd)
            {
                ByteBuffer cellName = columnName.copy().add(bb).build();
                cf.addColumn(params.makeColumn(cellName, ByteBufferUtil.EMPTY_BYTE_BUFFER));
            }
            */
        }
    }
}
