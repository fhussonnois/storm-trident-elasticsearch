package com.github.fhuss.storm.elasticsearch.functions;

import backtype.storm.tuple.Values;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class CreateJson extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            collector.emit(new Values(new ObjectMapper().writeValueAsString(tuple.getValue(0))));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}