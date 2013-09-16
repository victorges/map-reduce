package com.v1ct04.bigdata;

public interface Mapper<InKey, InValue, OutKey, OutValue> {

    public void map(Yielder<Pair<OutKey, OutValue>> yielder, InKey key, InValue value)
        throws Exception;
}
