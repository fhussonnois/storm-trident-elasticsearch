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
