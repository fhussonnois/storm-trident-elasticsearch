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
