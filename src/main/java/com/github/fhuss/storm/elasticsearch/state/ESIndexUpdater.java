package com.github.fhuss.storm.elasticsearch.state;

import com.github.fhuss.storm.elasticsearch.handler.BulkResponseHandler;
import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.handler.BulkResponseHandler.LoggerResponseHandler;
import com.github.fhuss.storm.elasticsearch.mapper.TridentTupleMapper;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * Simple {@link BaseStateUpdater} implementation for Elasticsearch.
 *
 * @author fhussonnois
 */
public class ESIndexUpdater<T> extends BaseStateUpdater<ESIndexState> {

    private final TridentTupleMapper<Document<T>> documentTupleMapper;
    private final BulkResponseHandler bulkResponseHandler;

    public ESIndexUpdater(TridentTupleMapper<Document<T>> documentTupleMapper) {
        this(documentTupleMapper, new LoggerResponseHandler());
    }

    public ESIndexUpdater(TridentTupleMapper<Document<T>> docBuilder, BulkResponseHandler bulkResponseHandler) {
        this.documentTupleMapper = docBuilder;
        this.bulkResponseHandler = bulkResponseHandler;
    }

    public void updateState(ESIndexState state, List<TridentTuple> inputs, TridentCollector collector) {
        state.bulkUpdateIndices(inputs, documentTupleMapper, bulkResponseHandler);
    }
}
