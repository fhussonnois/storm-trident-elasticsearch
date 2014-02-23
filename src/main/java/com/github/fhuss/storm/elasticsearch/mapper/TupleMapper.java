package com.github.fhuss.storm.elasticsearch.mapper;

import backtype.storm.tuple.Tuple;

/**
 * Interface for building document from {@link Tuple}.
 * @param <T>

 */
public interface TupleMapper<T> {

   T map(Tuple input);
}