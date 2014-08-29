package com.github.fhuss.storm.elasticsearch.bolt;

import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.github.fhuss.storm.elasticsearch.BaseLocalClusterTest;
import com.github.fhuss.storm.elasticsearch.mapper.impl.DefaultTupleMapper;
import static com.github.fhuss.storm.elasticsearch.mapper.impl.DefaultTupleMapper.*;

import com.github.fhuss.storm.elasticsearch.model.Tweet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author fhussonnois
 */
public class IndexBatchBoltTest extends BaseLocalClusterTest {

    public IndexBatchBoltTest() {
        super("my_index");
    }

    @Test
    public void shouldExecuteBulkRequestAfterReceivingTickTuple() {
        Assert.assertEquals(StaticSpout.MSGS.length, esSetup.countAll().intValue());
    }

    @Override
    public StormTopology buildTopology() {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("batch", new StaticSpout()).setMaxTaskParallelism(1);
        builder.setBolt("index", newIndexBatchBolt()).shuffleGrouping("batch");

        return  builder.createTopology();
    }

    protected IndexBatchBolt<String> newIndexBatchBolt( ) {
        DefaultTupleMapper mapper = DefaultTupleMapper.newObjectDefaultTupleMapper();
        return new IndexBatchBolt<>(getLocalClient(), mapper, 5, TimeUnit.SECONDS);
    }

    public static class StaticSpout extends BaseRichSpout {

        public static String[] MSGS = {
               "the cow jumped over the moon",
               "the man went to the store and bought some candy",
               "four score and seven years ago",
               "how many apples can you eat",
               "to be or not to be the person"
        };

        private SpoutOutputCollector spoutOutputCollector;
        private int current = 0;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields(FIELD_NAME, FIELD_TYPE, FIELD_SOURCE, FIELD_ID, FIELD_PARENT_ID));
        }

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector = spoutOutputCollector;
        }

        @Override
        public void nextTuple() {
            if( current < MSGS.length ) {
                spoutOutputCollector.emit(new Values("my_index", "my_type", new Tweet(MSGS[current], 0), String.valueOf(MSGS[current].hashCode()), null));
                current++;
            }
        }
    }
}