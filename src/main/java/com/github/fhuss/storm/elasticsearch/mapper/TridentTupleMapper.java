package com.github.fhuss.storm.elasticsearch.mapper;

import storm.trident.tuple.TridentTuple;

import java.io.Serializable;

/**
 * Interface for building document from {@link storm.trident.tuple.TridentTuple}.
 * @param <T>
 */
public interface TridentTupleMapper<T> extends Serializable {

   T map(TridentTuple input);
}