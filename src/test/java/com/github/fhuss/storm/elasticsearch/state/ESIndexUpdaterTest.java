package com.github.fhuss.storm.elasticsearch.state;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fhuss.storm.elasticsearch.BaseLocalClusterTest;
import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.functions.CreateJson;
import com.github.fhuss.storm.elasticsearch.functions.ExtractSearchArgs;
import com.github.fhuss.storm.elasticsearch.mapper.TridentTupleMapper;
import com.github.fhuss.storm.elasticsearch.model.Tweet;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Assert;
import org.junit.Test;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;

/**
 * @author fhussonnois
 */
public class ESIndexUpdaterTest extends BaseLocalClusterTest {

    static final ObjectMapper MAPPER = new ObjectMapper();

    public ESIndexUpdaterTest() {
        super("my_index");
    }

    @Test
    public void shouldExecuteDRPC( ) throws IOException {
        String query1 = QueryBuilders.termQuery("text", "moon").buildAsBytes().toUtf8();
        String query2 = QueryBuilders.termQuery("text", "man").buildAsBytes().toUtf8();
        String query3 = QueryBuilders.termQuery("text", "score").buildAsBytes().toUtf8();
        String query4 = QueryBuilders.termQuery("text", "apples").buildAsBytes().toUtf8();
        String query5 = QueryBuilders.termQuery("text", "person").buildAsBytes().toUtf8();

        assertDRPC(drpc.execute("search", query1 + " my_index my_type"), "the cow jumped over the moon");
        assertDRPC(drpc.execute("search", query2 + " my_index my_type"), "the man went to the store and bought some candy");
        assertDRPC(drpc.execute("search", query3 + " my_index my_type"), "four score and seven years ago");
        assertDRPC(drpc.execute("search", query4 + " my_index my_type"), "how many apples can you eat");
        assertDRPC(drpc.execute("search", query5 + " my_index my_type"), "to be or not to be the person");

    }

    protected void assertDRPC(String actual, String expected) throws IOException {
        String s = MAPPER.readValue(actual, String[][].class)[0][0];
        Assert.assertEquals(expected, MAPPER.readValue(s, Tweet.class).getText());
    }

    @Override
    protected StormTopology buildTopology() {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("to be or not to be the person"));
        spout.setCycle(true);

        ESIndexState.Factory<Tweet> factory = new ESIndexState.Factory<>(getLocalClient(), Tweet.class);
        TridentTopology topology = new TridentTopology();

        TridentState state = topology.newStream("tweets", spout)
                .partitionPersist(factory, new Fields("sentence"), new ESIndexUpdater(new MyTridentTupleMapper()));

        topology.newDRPCStream("search", drpc)
                .each(new Fields("args"), new ExtractSearchArgs(), new Fields("query", "indices", "types"))
                .groupBy(new Fields("query", "indices", "types"))
                .stateQuery(state, new Fields("query", "indices", "types"), new QuerySearchIndexQuery(), new Fields("tweet"))
                .each(new Fields("tweet"), new FilterNull())
                .each(new Fields("tweet"), new CreateJson(), new Fields("json"))
                .project(new Fields("json"));

        return topology.build();
    }


    public static class MyTridentTupleMapper implements TridentTupleMapper<Document<Tweet>> {

        @Override
        public Document<Tweet> map(TridentTuple input) {
            String sentence = input.getStringByField("sentence");
            return new Document<>("my_index", "my_type", new Tweet(sentence, 0), String.valueOf(sentence.hashCode()), null);
        }
    }
}
