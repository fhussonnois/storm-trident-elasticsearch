package com.github.fhuss.storm.elasticsearch.mapper;

import storm.trident.tuple.TridentTuple;

/**
 * Interface for building document from {@link storm.trident.tuple.TridentTuple}.
 * @param <T>

 */
public interface TridentTupleMapper<T> {

   T map(TridentTuple input);
}