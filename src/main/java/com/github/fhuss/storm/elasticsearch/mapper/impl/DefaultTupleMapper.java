package com.github.fhuss.storm.elasticsearch.mapper.impl;

import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.mapper.MappingException;
import com.github.fhuss.storm.elasticsearch.mapper.TupleMapper;

/**
 * Default mapper that attempt to map tuple fields to a {@link Document}.
 *
 * @author fhussonnois
 */
public class DefaultTupleMapper<T> implements TupleMapper<Document<T>> {

    public static final String FIELD_SOURCE      = "source";
    public static final String FIELD_NAME        = "name";
    public static final String FIELD_TYPE        = "type";
    public static final String FIELD_PARENT_ID   = "parentId";
    public static final String FIELD_ID          = "id";

    private TupleMapper<T> sourceMapperStrategy;

    private DefaultTupleMapper(TupleMapper<T> sourceMapperStrategy) {
        this.sourceMapperStrategy = sourceMapperStrategy;
    }

    /**
     * Returns a new {@link DefaultTupleMapper} that accept String as source field value.
     */
    public static final DefaultTupleMapper<String> newStringDefaultTupleMapper( ) {
        return new DefaultTupleMapper<>(new TupleMapper<String>() {
            @Override
            public String map(Tuple input) {
                return input.getStringByField(FIELD_SOURCE);
            }
        });
    }
    /**
     * Returns a new {@link DefaultTupleMapper} that accept Byte[] as source field value.
     */
    public static final DefaultTupleMapper<Byte[]> newBinaryDefaultTupleMapper( ) {
        return new DefaultTupleMapper<>(new TupleMapper<Byte[]>() {
            @Override
            public Byte[] map(Tuple input) {
                byte[] binaryByField = input.getBinaryByField(FIELD_SOURCE);

                Byte[] source = new Byte[binaryByField.length];
                for(int i = 0; i < binaryByField.length; i++)
                    source[i] = binaryByField[i];
                return source;
            }
        });
    }

    /**
     * Returns a new {@link DefaultTupleMapper} that accept Object as source field value.
     */
    public static final DefaultTupleMapper<String> newObjectDefaultTupleMapper( ) {
        final ObjectMapper mapper = new ObjectMapper();
        return new DefaultTupleMapper<>(new TupleMapper<String>() {
            @Override
            public String map(Tuple input) {
                try {
                    return mapper.writeValueAsString(input.getValueByField(FIELD_SOURCE));
                } catch (JsonProcessingException e) {
                    throw new MappingException("Error happen while processing json on object", e);
                }
            }
        });
    }

    @Override
    public Document<T> map(Tuple input) {
        String id   = input.getStringByField(FIELD_ID);
        String name = input.getStringByField(FIELD_NAME);
        String type = input.getStringByField(FIELD_TYPE);
        String parentId = input.getStringByField(FIELD_PARENT_ID);

        return new Document<>(name, type, sourceMapperStrategy.map(input), id, parentId);
    }
}
