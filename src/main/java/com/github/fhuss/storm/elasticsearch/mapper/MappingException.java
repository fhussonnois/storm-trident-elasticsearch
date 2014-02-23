package com.github.fhuss.storm.elasticsearch.mapper;


public class MappingException extends RuntimeException {

    public MappingException(String message, Throwable source) {
        super(message, source);
    }
}
