package com.github.fhuss.storm.elasticsearch.bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.mapper.TupleMapper;
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

/**
 * Simple Bolt to index documents batch into an elasticsearch cluster.
 *
 * @author fhussonnois
 */
public class IndexBatchBolt<T> implements IRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexBatchBolt.class);

    private static final int QUEUE_MAX_SIZE = 1000;

    private OutputCollector outputCollector;

    private Client client;

    private ClientFactory clientFactory;

    private LinkedBlockingQueue<Tuple> queue;

    private TupleMapper<Document<T>> mapper;

    public IndexBatchBolt(ClientFactory clientFactory, TupleMapper<Document<T>> mapper) {
        this.clientFactory = clientFactory;
        this.mapper = mapper;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.client = clientFactory.makeClient(stormConf);
        this.queue = new LinkedBlockingQueue<>(QUEUE_MAX_SIZE);
    }

    @Override
    public void execute(Tuple tuple) {

        if( isTickTuple(tuple)) {
            bulkUpdateIndexes();
            outputCollector.ack(tuple);
        } else if( ! queue.offer(tuple) ) {
            bulkUpdateIndexes();
            queue.add(tuple);
        }
    }

    protected void bulkUpdateIndexes( ) {

        List<Tuple> inputs = new ArrayList<>(queue.size());
        queue.drainTo(inputs);
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (Tuple input : inputs) {
            Document doc = mapper.map(input);
            IndexRequestBuilder request = client.prepareIndex(doc.getName(), doc.getType(), doc.getId()).setSource(doc.getSource().toString());

            if(doc.getParentId() != null) {
                request.setParent(doc.getParentId());
            }
            bulkRequest.add(request);
        }

        if( bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
            if( bulkItemResponses.hasFailures()) failAll(inputs) ; else ackAll(inputs);
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

    private static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return conf;
    }
}