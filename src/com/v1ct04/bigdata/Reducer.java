package com.v1ct04.bigdata;

public interface Reducer<Key, InValue, OutValue> {

    public OutValue reduce(Key key, Iterable<InValue> value) throws Exception;
}
