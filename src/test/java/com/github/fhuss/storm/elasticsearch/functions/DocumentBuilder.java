package com.github.fhuss.storm.elasticsearch.functions;


import backtype.storm.tuple.Values;
import com.github.fhuss.storm.elasticsearch.Document;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class DocumentBuilder extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String sentence = tuple.getString(0);
        collector.emit(new Values( new Document<>("my_index", "my_type", sentence, String.valueOf(sentence.hashCode()))));
    }
}
