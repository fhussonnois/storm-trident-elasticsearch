/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.fhuss.storm.elasticsearch.state;

import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.ReportedFailedException;
import backtype.storm.tuple.Values;
import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.handler.BulkResponseHandler;
import com.google.common.base.Objects;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import static com.github.fhuss.storm.elasticsearch.state.ValueSerializer.*;

/**
 * This class implements Trident State on top of ElasticSearch.
 * It follows trident-memcached library (https://github.com/nathanmarz/trident-memcached) as a template.
 *
 * @author fhussonnois
 * @param <T> OpaqueValue, TransactionalValue or any other non transactional type
 */
public class ESIndexMapState<T> implements IBackingMap<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ESIndexMapState.class);

    public static class Options extends HashMap<String, String> {

        private static final int DEFAULT_CACHE_SIZE = 1000;
        private static final String DEFAULT_GLOBAL_KEY = "GLOBAL$KEY";
        public static final String REPORT_ERROR = "trident.elasticsearch.state.report.error";
        public static final String CACHE_SIZE   = "trident.elasticsearch.state.cache.size";
        public static final String GLOBAL_KEY   = "trident.elasticsearch.state.global.key";

        public Options(Map<String, String> conf) {
            super(conf);
        }
        public boolean reportError() {
            return Boolean.valueOf(get(REPORT_ERROR));
        }
        public int getCachedMapSize( ) {
            String cacheSize = get(CACHE_SIZE);
            return cacheSize != null ? Integer.valueOf(cacheSize) : DEFAULT_CACHE_SIZE;

        }
        public String getGlobalKey( ) {
            String globalKey = get(GLOBAL_KEY);
            return globalKey != null ? globalKey : DEFAULT_GLOBAL_KEY;
        }
    }

    public static <T> Factory<OpaqueValue<T>> opaque(ClientFactory client, Class<T> type) {
        return new OpaqueFactory<>(client, StateType.OPAQUE, new OpaqueValueSerializer<>(type));
    }

    public static <T> Factory<TransactionalValue<T>> transactional(ClientFactory client, Class<T> type) {
        return new TransactionalFactory<>(client, StateType.TRANSACTIONAL, new TransactionalValueSerializer<>(type));
    }

    public static <T> Factory<T> nonTransactional(ClientFactory client, Class<T> type) {
        return new NonTransactionalFactory<>(client, StateType.NON_TRANSACTIONAL, new NonTransactionalValueSerializer<>(type));
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
            Options options = new Options(conf);
            ESIndexMapState<OpaqueValue<T>> mapState = new ESIndexMapState<>(clientFactory.makeClient(conf), serializer, new BulkResponseHandler.LoggerResponseHandler(), options.reportError());
            MapState ms  = OpaqueMap.build(new CachedMap(mapState, options.getCachedMapSize()));
            return new SnapshottableMap<OpaqueValue<T>>(ms, new Values(options.getGlobalKey()));
        }
    }

    public static class TransactionalFactory<T> extends Factory<TransactionalValue<T>> {

        public TransactionalFactory(ClientFactory clientFactory, StateType stateType, ValueSerializer<TransactionalValue<T>> serializer) {
            super(clientFactory, stateType, serializer);
        }

        @Override
        public State makeState(Map conf, IMetricsContext iMetricsContext, int i, int i2) {
            Options options = new Options(conf);
            ESIndexMapState<TransactionalValue<T>> mapState = new ESIndexMapState<>(clientFactory.makeClient(conf), serializer, new BulkResponseHandler.LoggerResponseHandler(), options.reportError());
            MapState<T> ms  = TransactionalMap.build(new CachedMap(mapState, options.getCachedMapSize()));
            Values snapshotKey = new Values(options.getGlobalKey());
            return new SnapshottableMap<>(ms, snapshotKey);
        }
    }

    public static class NonTransactionalFactory<T> extends Factory<T> {

        public NonTransactionalFactory(ClientFactory clientFactory, StateType stateType, ValueSerializer<T> serializer) {
            super(clientFactory, stateType, serializer);
        }

        @Override
        public State makeState(Map conf, IMetricsContext iMetricsContext, int i, int i2) {
            Options options = new Options(conf);
            ESIndexMapState<T> mapState = new ESIndexMapState<>(clientFactory.makeClient(conf), serializer, new BulkResponseHandler.LoggerResponseHandler(), options.reportError());
            MapState<T> ms  = NonTransactionalMap.build(new CachedMap<>(mapState, options.getCachedMapSize()));
            return new SnapshottableMap<>(ms, new Values(options.getGlobalKey()));
        }
    }

    private BulkResponseHandler bulkResponseHandler;
    private ValueSerializer<T> serializer;
    private Client client;

    private boolean reportError;

    public ESIndexMapState(Client client, ValueSerializer<T> serializer, BulkResponseHandler bulkResponseHandler, boolean reportError) {
        this.client = client;
        this.serializer = serializer;
        this.bulkResponseHandler = bulkResponseHandler;
        this.reportError = reportError;
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
            MultiGetResponse multiGetResponses;
            try {
                multiGetResponses = request.execute().actionGet();
            } catch (ElasticsearchException e) {
               String error = "Failed to read data into elasticsearch";
                throw (reportError) ? new ReportedFailedException(error, e) : new FailedException(error, e);
            }
            for(MultiGetItemResponse itemResponse : multiGetResponses.getResponses()) {
                GetResponse res = itemResponse.getResponse();
                if( res != null && !res.isSourceEmpty()) {
                    try {
                        responses.add(serializer.deserialize(res.getSourceAsBytes()));
                    } catch (IOException e) {
                        LOGGER.error("error while trying to deserialize data from json", e);
                        responses.add(null);
                    }
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
            try {
                byte[] source = serializer.serialize(value);
                bulkRequestBuilder.add(client.prepareIndex(groupBy.index, groupBy.type, groupBy.id).setSource(source));
            } catch (IOException e) {
               LOGGER.error("Oops data loss - error while trying to serialize data to json", e);
            }
        }

        try {
            bulkResponseHandler.handle(bulkRequestBuilder.execute().actionGet());
        } catch(ElasticsearchException e) {
            LOGGER.error("error while executing bulk request to elasticsearch");
            String error = "Failed to store data into elasticsearch";
            throw (reportError) ? new ReportedFailedException(error, e) : new FailedException(error, e);
        }
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

        public String toString( ) {
            return Objects.toStringHelper(this)
                    .add("index", index)
                    .add("type", type)
                    .add("id", id).toString();
        }
    }
}
