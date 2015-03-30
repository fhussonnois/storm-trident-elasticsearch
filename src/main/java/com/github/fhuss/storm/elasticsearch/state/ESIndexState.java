package com.github.fhuss.storm.elasticsearch.state;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.FailedException;

import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.handler.BulkResponseHandler;
import com.github.fhuss.storm.elasticsearch.mapper.TridentTupleMapper;

/**
 * Simple {@link State} implementation for Elasticsearch.
 *
 * @author fhussonnois
 */
public class ESIndexState<T> implements State {

    public static final Logger LOGGER = LoggerFactory.getLogger(ESIndexState.class);

    private Client client;

    private ValueSerializer<T> serializer;

    public ESIndexState(Client client, ValueSerializer<T> serializer) {
        this.client = client;
        this.serializer = serializer;
    }

    @Override
    public void beginCommit(Long aLong) {

    }

    @Override
    public void commit(Long aLong) {

    }

    public void bulkUpdateIndices(List<TridentTuple> inputs, TridentTupleMapper<Document<T>> mapper, BulkResponseHandler handler) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (TridentTuple input : inputs) {
            Document<T> doc = mapper.map(input);
            byte[] source = serializeSourceOrFail(doc);
            IndexRequestBuilder request = client.prepareIndex(doc.getName(), doc.getType(), doc.getId()).setSource(source);

            if(doc.getParentId() != null) {
                request.setParent(doc.getParentId());
            }
            bulkRequest.add(request);
        }

        if( bulkRequest.numberOfActions() > 0) {
            try {
                handler.handle(bulkRequest.execute().actionGet());
            } catch(ElasticsearchException e) {
                LOGGER.error("error while executing bulk request to elasticsearch");
                throw new FailedException("Failed to store data into elasticsearch", e);
            }
        }
    }

    protected byte[] serializeSourceOrFail(Document<T> doc) {
        try {
            return serializer.serialize(doc.getSource());
        } catch (IOException e) {
            LOGGER.error("Error while serializing document source", e);
            throw new FailedException("Failed to serialize source as byte[]", e);
        }
    }

    public Collection<T> searchQuery(String query, List<String> indices, List<String> types) {
        return searchQuery(query, indices, types, 10);
    }
    
    public Collection<T> searchQuery(String query, List<String> indices, List<String> types, int size) {
        SearchResponse response = buildSearchQuery(query, indices, types, size).execute().actionGet();
        return buildResult(response);
    }

    public Collection<T> searchSortedAndFirstNQuery(String query, List<String> indices, List<String> types,
                                                    SortBuilder sortBuilder, int firstN) {
        SearchResponse response = buildSearchQuery(query, indices, types, firstN)
                .addSort(sortBuilder)
                .setQuery(query).execute().actionGet();

        return buildResult(response);
    }

    private SearchRequestBuilder buildSearchQuery(String query, List<String> indices, List<String> types, int size) {
        return client.prepareSearch()
                .setIndices(indices.toArray(new String[indices.size()]))
                .setTypes(types.toArray(new String[types.size()]))
                .setSize(size)
                .setQuery(query);
    }
    
    private List<T> buildResult(SearchResponse response) {
        List<T> result = new LinkedList<>();
        for(SearchHit hit : response.getHits()) {
            try {
                result.add(serializer.deserialize(hit.source()));
            } catch (IOException e) {
                LOGGER.error("Error while trying to deserialize data from json source");
            }
        }
        return result;
    }

    public static class Factory<T> implements StateFactory {

        private ClientFactory clientFactory;
        private ValueSerializer<T> serializer;

        public Factory(ClientFactory clientFactory, Class<T> clazz ) {
            this.clientFactory = clientFactory;
            this.serializer = new ValueSerializer.NonTransactionalValueSerializer<>(clazz);
        }

        @Override
        public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i2) {
            return new ESIndexState<>(makeClient(map), serializer);
        }

        protected Client makeClient(Map map) {
            return clientFactory.makeClient(map);
        }
    }
}