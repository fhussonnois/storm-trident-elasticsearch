package com.github.fhuss.storm.elasticsearch.mapper;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;

/**
 * Interface for building document from {@link Tuple}.
 * @param <T>

 */
public interface TupleMapper<T> extends Serializable {

   T map(Tuple input);
}