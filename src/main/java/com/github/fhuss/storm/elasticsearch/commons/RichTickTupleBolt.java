package com.github.fhuss.storm.elasticsearch.commons;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A simple {@link backtype.storm.topology.base.BaseBasicBolt} implementation with tick tuple support.
 *
 * @author fhussonnois
 *
 */
public abstract class RichTickTupleBolt implements IRichBolt {

    private long emitFrequency;

    /**
     * Creates a new {@link RichTickTupleBolt} instance.
     * @param emitFrequency the tick tuple emit frequency
     * @param unit the time unit of the emit frequency
     */
    public RichTickTupleBolt(long emitFrequency, TimeUnit unit) {
        this.emitFrequency = unit.toSeconds(emitFrequency);
    }


    private static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
        return conf;
    }

    @Override
    public void execute(Tuple tuple) {
        if( isTickTuple(tuple) ) {
            executeTickTuple(tuple);
        } else {
            executeTuple(tuple);
        }
    }

    protected abstract void executeTickTuple(Tuple tuple);

    protected abstract void executeTuple(Tuple tuple);
}
