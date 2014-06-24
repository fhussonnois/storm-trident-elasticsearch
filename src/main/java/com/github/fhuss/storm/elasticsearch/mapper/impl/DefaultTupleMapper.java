package com.github.fhuss.storm.elasticsearch.mapper.impl;

import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.mapper.MappingException;
import com.github.fhuss.storm.elasticsearch.mapper.TupleMapper;

import java.io.UnsupportedEncodingException;

/**
 * Default mapper that attempt to map tuple fields to a {@link Document}.
 *
 * @author fhussonnois
 */
public class DefaultTupleMapper implements TupleMapper<Document<String>> {

    public static final String FIELD_SOURCE      = "source";
    public static final String FIELD_NAME        = "name";
    public static final String FIELD_TYPE        = "type";
    public static final String FIELD_PARENT_ID   = "parentId";
    public static final String FIELD_ID          = "id";

    private TupleMapper<String> sourceMapperStrategy;

    private DefaultTupleMapper(TupleMapper<String> sourceMapperStrategy) {
        this.sourceMapperStrategy = sourceMapperStrategy;
    }

    /**
     * Returns a new {@link DefaultTupleMapper} that accept String as source field value.
     */
    public static final DefaultTupleMapper newStringDefaultTupleMapper( ) {
        return new DefaultTupleMapper(new TupleMapper<String>() {
            @Override
            public String map(Tuple input) {
                return input.getStringByField(FIELD_SOURCE);
            }
        });
    }
    /**
     * Returns a new {@link DefaultTupleMapper} that accept Byte[] as source field value.
     */
    public static final DefaultTupleMapper newBinaryDefaultTupleMapper( ) {
        return new DefaultTupleMapper(new TupleMapper<String>() {
            @Override
            public String map(Tuple input) {
                try {
                    return new String(input.getBinaryByField(FIELD_SOURCE), "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new MappingException("Error while processing source as a byte[]", e);
                }
            }
        });
    }

    /**
     * Returns a new {@link DefaultTupleMapper} that accept Object as source field value.
     */
    public static final DefaultTupleMapper newObjectDefaultTupleMapper( ) {
        final ObjectMapper mapper = new ObjectMapper();
        return new DefaultTupleMapper(new TupleMapper<String>() {
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
    public Document<String> map(Tuple input) {
        String id   = input.getStringByField(FIELD_ID);
        String name = input.getStringByField(FIELD_NAME);
        String type = input.getStringByField(FIELD_TYPE);
        String parentId = ( input.contains(FIELD_PARENT_ID) ) ? input.getStringByField(FIELD_PARENT_ID) : null;

        return new Document<>(name, type, sourceMapperStrategy.map(input), id, parentId);
    }
}
