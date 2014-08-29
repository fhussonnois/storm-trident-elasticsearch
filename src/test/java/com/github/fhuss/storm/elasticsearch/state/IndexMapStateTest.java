package com.github.fhuss.storm.elasticsearch.state;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fhuss.storm.elasticsearch.BaseLocalClusterTest;
import com.github.fhuss.storm.elasticsearch.model.Tweet;
import com.google.common.collect.Lists;
import com.github.fhuss.storm.elasticsearch.Document;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.*;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;

/**
 * Default test class.
 *
 * @author fhussonnois
 */
public class IndexMapStateTest extends BaseLocalClusterTest{

    static final ObjectMapper MAPPER = new ObjectMapper();

    public IndexMapStateTest() {
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
        Assert.assertEquals(expected, MAPPER.readValue(s, Tweet.class).getText() );
    }

    public static class DocumentBuilder extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            collector.emit(new Values( new Document<>("my_index", "my_type", sentence, String.valueOf(sentence.hashCode()))));
        }
    }

    public static class ExtractDocumentInfo extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            Document t = (Document)tuple.getValue(0);
            collector.emit(new Values(t.getId(), t.getName(), t.getType()));
        }
    }

    public static class ExtractSearchArgs extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String args = (String)tuple.getValue(0);
            String[] split = args.split(" ");
            collector.emit(new Values(split[0], Lists.newArrayList(split[1]), Lists.newArrayList(split[2])));
        }
    }

    public static class CreateJson extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            try {
                collector.emit(new Values(new ObjectMapper().writeValueAsString(tuple.getValue(0))));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class TweetBuilder implements ReducerAggregator<Tweet> {
        @Override
        public Tweet init() {
            return null;
        }

        @Override
        public Tweet reduce(Tweet tweet, TridentTuple objects) {

            Document<String> doc  = (Document) objects.getValueByField("document");
            if( tweet == null)
                tweet = new Tweet(doc.getSource(), 1);
            else {
                tweet.incrementCount();
            }

            return tweet;
        }
    }

    @Override
    public StormTopology buildTopology( ) {
        ESIndexMapState.Factory<Tweet> state = ESIndexMapState.nonTransactional(getLocalClient(), Tweet.class);

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("to be or not to be the person"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();

        TridentState staticState = topology.newStaticState(new ESIndexState.Factory<>(getLocalClient(), Tweet.class));

        topology.newStream("tweets", spout)
                        .each(new Fields("sentence"), new DocumentBuilder(), new Fields("document"))
                        .each(new Fields("document"), new ExtractDocumentInfo(), new Fields("id", "index", "type"))
                        .groupBy(new Fields("index", "type", "id"))
                        .persistentAggregate(state, new Fields("document"), new TweetBuilder(), new Fields("tweet"))
                        .parallelismHint(1);

        topology.newDRPCStream("search", drpc)
                .each(new Fields("args"), new ExtractSearchArgs(), new Fields("query", "indices", "types"))
                .groupBy(new Fields("query", "indices", "types"))
                .stateQuery(staticState, new Fields("query", "indices", "types"), new QuerySearchIndexQuery(), new Fields("tweet"))
                .each(new Fields("tweet"), new FilterNull())
                .each(new Fields("tweet"), new CreateJson(), new Fields("json"))
                .project(new Fields("json"));

        return topology.build();
    }
}
