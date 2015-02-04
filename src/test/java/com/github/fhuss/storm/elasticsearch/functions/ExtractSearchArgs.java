package com.github.fhuss.storm.elasticsearch.functions;

import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class ExtractSearchArgs extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String args = (String)tuple.getValue(0);
        String[] split = args.split(" ");
        collector.emit(new Values(split[0], Lists.newArrayList(split[1]), Lists.newArrayList(split[2])));
    }
}