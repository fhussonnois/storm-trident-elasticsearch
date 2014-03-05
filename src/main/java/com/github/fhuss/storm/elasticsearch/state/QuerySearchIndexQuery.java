package com.github.fhuss.storm.elasticsearch.state;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Simple {@link BaseQueryFunction} to execute elasticsearch query search.
 *
 * @author fhussonnois
 */
public class QuerySearchIndexQuery<T> extends BaseQueryFunction<ESIndexState<T>, Collection<T>> {

    @Override
    public List<Collection<T>> batchRetrieve(ESIndexState<T> indexState, List<TridentTuple> inputs) {
        List<Collection<T>> res = new LinkedList<>( );
        for(TridentTuple input : inputs) {
            String query = (String)input.getValueByField("query");
            List<String> types = (List) input.getValueByField("types");
            List<String> indices = (List) input.getValueByField("indices");
            res.add(indexState.searchQuery(query, indices, types));
        }
        return res;
    }

    @Override
    public void execute(TridentTuple objects, Collection<T> tl, TridentCollector tridentCollector) {
        for(T t :  tl) tridentCollector.emit( new Values(t));
    }
}
