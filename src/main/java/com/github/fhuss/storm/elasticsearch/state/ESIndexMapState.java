package com.github.fhuss.storm.elasticsearch.state;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;
import com.github.fhuss.storm.elasticsearch.handler.BulkResponseHandler;
import com.github.fhuss.storm.elasticsearch.ClientFactory;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Client;

import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.map.*;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.ListIterator;

import static com.github.fhuss.storm.elasticsearch.state.ValueSerializer.*;

/**
 * This class implements Trident State on top of ElasticSearch.
 * It follows trident-memcached library (https://github.com/nathanmarz/trident-memcached) as a template.
 *
 * @author fhussonnois
 * @param <T> OpaqueValue, TransactionalValue or any other non transactional type
 */
public class ESIndexMapState<T> implements IBackingMap<T> {

    private static final int cacheSize = 1000;

    private static final String globalKey = "GLOBAL$KEY";


    public static <T> Factory<OpaqueValue<T>> opaque(ClientFactory client, Class<T> type) {
        return new OpaqueFactory(client, StateType.OPAQUE, new OpaqueValueSerializer(type));
    }

    public static <T> Factory<TransactionalFactory<T>> transactional(ClientFactory client, Class<T> type) {
        return new TransactionalFactory(client, StateType.TRANSACTIONAL, new TransactionalValueSerializer(type));
    }

    public static <T> Factory<T> nonTransactional(ClientFactory client, Class<T> type) {
        return new NonTransactionalFactory(client, StateType.NON_TRANSACTIONAL, new NonTransactionalValueSerializer(type));
    }

    public abstract static class Factory<T> implements StateFactory {
        protected ValueSerializer<T> serializer;
        protected ClientFactory clientFactory;
        protected StateType stateType;

        public Factory(ClientFactory clientFactory, StateType stateType, ValueSerializer<T> serializer) {
            this.clientFactory = clientFactory;
            this.stateType = stateType;
            this.serializer = serializer;
        }
    }

    public static class OpaqueFactory<T> extends Factory<OpaqueValue<T>> {

        public OpaqueFactory(ClientFactory clientFactory, StateType stateType, ValueSerializer<OpaqueValue<T>> serializer) {
            super(clientFactory, stateType, serializer);
        }

        @Override
        public State makeState(Map conf, IMetricsContext iMetricsContext, int i, int i2) {

            ESIndexMapState mapState = new ESIndexMapState(clientFactory.makeClient(conf), serializer, new BulkResponseHandler.LoggerResponseHandler());
            MapState ms  = OpaqueMap.build(new CachedMap(mapState, cacheSize));
            return new SnapshottableMap(ms, new Values(globalKey));
        }
    }

    public static class TransactionalFactory<T> extends Factory<TransactionalFactory<T>> {

        public TransactionalFactory(ClientFactory clientFactory, StateType stateType, ValueSerializer<TransactionalFactory<T>> serializer) {
            super(clientFactory, stateType, serializer);
        }

        @Override
        public State makeState(Map conf, IMetricsContext iMetricsContext, int i, int i2) {

            ESIndexMapState mapState = new ESIndexMapState(clientFactory.makeClient(conf), serializer, new BulkResponseHandler.LoggerResponseHandler());
            MapState ms  = TransactionalMap.build(new CachedMap(mapState, cacheSize));
            return new SnapshottableMap(ms, new Values(globalKey));
        }
    }

    public static class NonTransactionalFactory<T> extends Factory<TransactionalFactory<T>> {

        public NonTransactionalFactory(ClientFactory clientFactory, StateType stateType, ValueSerializer<TransactionalFactory<T>> serializer) {
            super(clientFactory, stateType, serializer);
        }

        @Override
        public State makeState(Map conf, IMetricsContext iMetricsContext, int i, int i2) {

            ESIndexMapState mapState = new ESIndexMapState(clientFactory.makeClient(conf), serializer, new BulkResponseHandler.LoggerResponseHandler());
            MapState ms  = NonTransactionalMap.build(new CachedMap(mapState, cacheSize));
            return new SnapshottableMap(ms, new Values(globalKey));
        }
    }

    private BulkResponseHandler bulkResponseHandler;
    private ValueSerializer<T> serializer;
    private Client client;

    public ESIndexMapState(Client client, ValueSerializer<T> serializer, BulkResponseHandler bulkResponseHandler) {
        this.client = client;
        this.serializer = serializer;
        this.bulkResponseHandler = bulkResponseHandler;
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<T> responses = new ArrayList<>(keys.size());

        List<GroupByKey> groupByKeys = new ArrayList<>(keys.size());
        for(List<Object> key : keys) {
            groupByKeys.add(GroupByKey.fromKeysList(key));
        }

        if( ! groupByKeys.isEmpty() ) {

            MultiGetRequestBuilder request = client.prepareMultiGet();
            for(GroupByKey key : groupByKeys) {
                request.add(key.index, key.type, key.id);
            }

            MultiGetResponse multiGetResponses = request.execute().actionGet();
            for(MultiGetItemResponse itemResponse : multiGetResponses.getResponses()) {
                GetResponse res = itemResponse.getResponse();
                if( res != null && !res.isSourceEmpty()) {
                    responses.add(serializer.deserialize(res.getSourceAsBytes()));
                } else {
                    responses.add(null);
                }
            }
        }
        return responses;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        ListIterator<T> listIterator = values.listIterator();
        while (listIterator.hasNext()) {
            GroupByKey groupBy = GroupByKey.fromKeysList(keys.get(listIterator.nextIndex()));
            T value = listIterator.next();

            byte[] source = serializer.serialize(value);
            bulkRequestBuilder.add(client.prepareIndex(groupBy.index, groupBy.type, groupBy.id).setSource(source));
        }

        bulkResponseHandler.handle(bulkRequestBuilder.execute().actionGet());
    }

    private static class GroupByKey {
        public final String index;
        public final String type;
        public final String id;

        public GroupByKey(String index, String type, String id) {
            this.index = index;
            this.type = type;
            this.id = id;
        }

        public static GroupByKey fromKeysList(List<Object> keys) {
            if( keys == null || keys.size() < 3) {
                throw new RuntimeException("Keys not supported " + keys);
            }
            return new GroupByKey(keys.get(0).toString(), keys.get(1).toString(), keys.get(2).toString());
        }
    }
}
