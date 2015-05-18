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
package com.github.fhuss.storm.elasticsearch.handler;

import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Interface to handle response after executing a bulk request.
 *
 * @author fhussonnois
 */
public interface BulkResponseHandler extends Serializable {

    final Logger LOGGER = LoggerFactory.getLogger(LoggerResponseHandler.class);

    void handle(BulkResponse response);

    public class LoggerResponseHandler implements BulkResponseHandler {

        @Override
        public void handle(BulkResponse response) {

            if( response.hasFailures() ) {
                LOGGER.error("BulkResponse has failures : {}", response.buildFailureMessage());
            }
        }
    }
}
