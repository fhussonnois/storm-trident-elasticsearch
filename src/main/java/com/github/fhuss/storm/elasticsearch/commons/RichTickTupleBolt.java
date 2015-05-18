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
package com.github.fhuss.storm.elasticsearch.commons;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A simple {@link backtype.storm.topology.base.BaseBasicBolt} implementation with tick tuple support.
 *
 * @author fhussonnois
 *
 */
public abstract class RichTickTupleBolt implements IRichBolt {

    private long emitFrequency;

    /**
     * Creates a new {@link RichTickTupleBolt} instance.
     * @param emitFrequency the tick tuple emit frequency
     * @param unit the time unit of the emit frequency
     */
    public RichTickTupleBolt(long emitFrequency, TimeUnit unit) {
        this.emitFrequency = unit.toSeconds(emitFrequency);
    }


    private static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
        return conf;
    }

    @Override
    public void execute(Tuple tuple) {
        if( isTickTuple(tuple) ) {
            executeTickTuple(tuple);
        } else {
            executeTuple(tuple);
        }
    }

    protected abstract void executeTickTuple(Tuple tuple);

    protected abstract void executeTuple(Tuple tuple);
}
