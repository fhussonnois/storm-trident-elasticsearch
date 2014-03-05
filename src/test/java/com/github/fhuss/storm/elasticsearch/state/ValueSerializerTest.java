package com.github.fhuss.storm.elasticsearch.state;

import org.junit.Assert;
import org.junit.Test;
import storm.trident.state.OpaqueValue;
import storm.trident.state.TransactionalValue;

import java.io.IOException;

import static com.github.fhuss.storm.elasticsearch.state.ValueSerializer.*;

/**
 * Unit tests for {@link ValueSerializer} implementations.
 *
 * @author fhussonnois
 */
public class ValueSerializerTest {

    public static class FooDocument {
        public String value;

        // dummy constructor
        public FooDocument() {

        }
        public FooDocument(String value) {
            this.value = value;
        }
    }

    @Test
    public void shouldDeSerializeNonTransactionValue( ) throws IOException {

        NonTransactionalValueSerializer<FooDocument> serializer = new NonTransactionalValueSerializer<>(FooDocument.class);
        byte[] value = serializer.serialize(new FooDocument("foo"));

        FooDocument actual = serializer.deserialize(value);
        Assert.assertNotNull(actual);
        Assert.assertEquals("foo", actual.value);
    }

    @Test
    public void shouldSerializeTransactionValue( ) throws IOException {
        TransactionalValueSerializer<FooDocument> serializer = new TransactionalValueSerializer<>(FooDocument.class);
        byte[] value = serializer.serialize(new TransactionalValue<>(1L, new FooDocument("foo")));

        TransactionalValue<FooDocument> actual = serializer.deserialize(value);
        Assert.assertNotNull(actual);
        Assert.assertEquals(1L, (long)actual.getTxid());
        Assert.assertEquals("foo", actual.getVal().value);
    }

    @Test
    public void shouldSerializeOpaqueValueWithNoPreviousValue( ) throws IOException {
        OpaqueValueSerializer<FooDocument> serializer = new OpaqueValueSerializer<>(FooDocument.class);
        byte[] value = serializer.serialize(new OpaqueValue<>(1L, new FooDocument("foo")));

        OpaqueValue<FooDocument> actual = serializer.deserialize(value);
        Assert.assertNotNull(actual);
        Assert.assertEquals(1L, (long)actual.getCurrTxid());
        Assert.assertEquals("foo", actual.getCurr().value);
    }

    @Test
    public void shouldSerializeOpaqueValueWithPreviousValue( ) throws IOException {
        OpaqueValueSerializer<FooDocument> serializer = new OpaqueValueSerializer<>(FooDocument.class);
        byte[] value = serializer.serialize(new OpaqueValue<>(1L, new FooDocument("foo"), new FooDocument("bar")));

        OpaqueValue<FooDocument> actual = serializer.deserialize(value);
        Assert.assertNotNull(actual);
        Assert.assertEquals(1L, (long)actual.getCurrTxid());
        Assert.assertEquals("foo", actual.getCurr().value);
        Assert.assertEquals("bar", actual.getPrev().value);
    }
}
