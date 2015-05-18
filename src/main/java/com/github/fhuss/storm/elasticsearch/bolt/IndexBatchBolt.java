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
package com.github.fhuss.storm.elasticsearch.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.commons.RichTickTupleBolt;
import com.github.fhuss.storm.elasticsearch.mapper.TupleMapper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Simple Bolt to index documents batch into an elasticsearch cluster.
 *
 * @author fhussonnois
 */
public class IndexBatchBolt<T> extends RichTickTupleBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexBatchBolt.class);

    public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;

    public static final long DEFAULT_EMIT_FREQUENCY = 10;

    private static final int QUEUE_MAX_SIZE = 1000;

    private OutputCollector outputCollector;

    private Client client;

    private ClientFactory clientFactory;

    private LinkedBlockingQueue<Tuple> queue;

    private TupleMapper<Document<T>> mapper;

    /**
     * Creates a new {@link IndexBatchBolt} instance.
     *
     * @param emitFrequency the batch frequency
     * @param unit the time unit of the emit frequency
     * @param clientFactory the elasticsearch client factory
     * @param mapper the document tuple mapper
     */
    public IndexBatchBolt(ClientFactory clientFactory, TupleMapper<Document<T>> mapper, long emitFrequency, TimeUnit unit) {
        super(emitFrequency, unit);
        this.clientFactory = clientFactory;
        this.mapper = mapper;
    }

    /**
     * Creates a new {@link IndexBatchBolt} instance which use SECOND as time unit for batch frequency.
     * @param clientFactory the elasticsearch client factory
     * @param mapper the the document tuple mapper
     */
    public IndexBatchBolt(ClientFactory clientFactory, TupleMapper<Document<T>> mapper, long emitFrequency) {
        this(clientFactory, mapper, emitFrequency, DEFAULT_TIME_UNIT);
    }

    /**
     * Creates a new {@link IndexBatchBolt} instance with a default batch frequency set to 10 seconds.
     * @param clientFactory the elasticsearch client factory
     * @param mapper the the document tuple mapper
     */
    public IndexBatchBolt(ClientFactory clientFactory, TupleMapper<Document<T>> mapper) {
        this(clientFactory, mapper, DEFAULT_EMIT_FREQUENCY, DEFAULT_TIME_UNIT);
    }

    /**
     * (non-Javadoc)
     * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
     */
    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.client = clientFactory.makeClient(stormConf);
        this.queue = new LinkedBlockingQueue<>(QUEUE_MAX_SIZE);
    }

    @Override
    protected void executeTickTuple(Tuple tuple) {
        bulkUpdateIndexes();
        outputCollector.ack(tuple);
    }

    @Override
    protected void executeTuple(Tuple tuple) {
        if( ! queue.offer(tuple) ) {
            bulkUpdateIndexes();
            queue.add(tuple);
        }
    }

    protected void bulkUpdateIndexes( ) {

        List<Tuple> inputs = new ArrayList<>(queue.size());
        queue.drainTo(inputs);
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (Tuple input : inputs) {
            Document<T> doc = mapper.map(input);
            IndexRequestBuilder request = client.prepareIndex(doc.getName(), doc.getType(), doc.getId()).setSource((String)doc.getSource());

            if(doc.getParentId() != null) {
                request.setParent(doc.getParentId());
            }
            bulkRequest.add(request);
        }

        try {
            if (bulkRequest.numberOfActions() > 0) {
                BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
                if (bulkItemResponses.hasFailures()) {
                    BulkItemResponse[] items = bulkItemResponses.getItems();
                    for (int i = 0; i < items.length; i++) {
                        ackOrFail(items[i], inputs.get(i));
                    }
                } else {
                    ackAll(inputs);
                }
            }
        } catch (ElasticsearchException e) {
            LOGGER.error("Unable to process bulk request, " + inputs.size() + " tuples are in failure", e);
            outputCollector.reportError(e.getRootCause());
            failAll(inputs);
        }
    }

    private void ackOrFail(BulkItemResponse item, Tuple tuple) {
        if (item.isFailed()) {
            LOGGER.error("Failed to process tuple : " + mapper.map(tuple));
            outputCollector.fail(tuple);
        } else {
            outputCollector.ack(tuple);
        }
    }

    protected void ackAll(List<Tuple> inputs) {
        for(Tuple t : inputs)
            outputCollector.ack(t);
    }

    protected void failAll(List<Tuple> inputs) {
        for(Tuple t : inputs)
            outputCollector.fail(t);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        /* no-ouput */
    }

    @Override
    public void cleanup() {
        if( this.client != null) this.client.close();
    }
}