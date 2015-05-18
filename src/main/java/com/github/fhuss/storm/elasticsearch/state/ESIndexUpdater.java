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

import com.github.fhuss.storm.elasticsearch.handler.BulkResponseHandler;
import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.handler.BulkResponseHandler.LoggerResponseHandler;
import com.github.fhuss.storm.elasticsearch.mapper.TridentTupleMapper;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * Simple {@link BaseStateUpdater} implementation for Elasticsearch.
 *
 * @author fhussonnois
 */
public class ESIndexUpdater<T> extends BaseStateUpdater<ESIndexState<T>> {

    private final TridentTupleMapper<Document<T>> documentTupleMapper;
    private final BulkResponseHandler bulkResponseHandler;

    public ESIndexUpdater(TridentTupleMapper<Document<T>> documentTupleMapper) {
        this(documentTupleMapper, new LoggerResponseHandler());
    }

    public ESIndexUpdater(TridentTupleMapper<Document<T>> docBuilder, BulkResponseHandler bulkResponseHandler) {
        this.documentTupleMapper = docBuilder;
        this.bulkResponseHandler = bulkResponseHandler;
    }

    public void updateState(ESIndexState<T> state, List<TridentTuple> inputs, TridentCollector collector) {
        state.bulkUpdateIndices(inputs, documentTupleMapper, bulkResponseHandler);
    }
}
