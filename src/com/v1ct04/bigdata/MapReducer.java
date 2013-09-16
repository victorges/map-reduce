package com.v1ct04.bigdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class MapReducer<MapKey, MapValue, OutKey, ReduceValue, OutValue> {

    private static final int THREAD_NUMBER = 16;
    private static final int MAPPER_THREADS = 8;

    private Mapper<MapKey, MapValue, OutKey, ReduceValue> mMapper;
    private Reducer<OutKey, ReduceValue, OutValue> mReducer;

    private Iterator<Pair<MapKey, MapValue>> mInputIterator;
    private Yielder<Pair<OutKey,ReduceValue>> mMapResults;

    private final Object mMappingsLock = new Object();
    private final AtomicInteger mMappingsCount = new AtomicInteger(0);
    private final AtomicInteger mReducingsCount = new AtomicInteger(0);

    public MapReducer(Mapper<MapKey, MapValue, OutKey, ReduceValue> mapper,
                      Reducer<OutKey, ReduceValue, OutValue> reducer) {
        mMapper = mapper;
        mReducer = reducer;
    }

    public synchronized Map<OutKey, OutValue> mapReduce(Iterable<Pair<MapKey, MapValue>> data) {
        if (mMapper == null) throw new IllegalStateException("Mapper not set");
        if (mReducer == null) throw new IllegalStateException("Reducer not set");
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUMBER);
        mInputIterator = data.iterator();
        mMapResults = new Yielder<>();
        final Map<OutKey, OutValue> finalResult = new HashMap<>();
        Map<OutKey, Yielder<ReduceValue>> reduceYielders = new HashMap<>();

        Runnable mapperRunnable = new MapperRunnable();
        for (int i = 0; i < MAPPER_THREADS; i++) {
            executorService.execute(mapperRunnable);
        }

        for (final Pair<OutKey, ReduceValue> mapResult : mMapResults) {
            final Yielder<ReduceValue> yielder;
            if (reduceYielders.containsKey(mapResult.first)) {
                yielder = reduceYielders.get(mapResult.first);
            } else {
                yielder = new Yielder<>();
                reduceYielders.put(mapResult.first, yielder);
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        mReducingsCount.incrementAndGet();
                        try {
                            finalResult.put(mapResult.first,
                                mReducer.reduce(mapResult.first, yielder));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        mReducingsCount.decrementAndGet();
                    }
                });
            }
            yielder.yield(mapResult.second);
        }
        for (Yielder<ReduceValue> yielder : reduceYielders.values()) yielder.finish();
        do {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while(mMappingsCount.get() > 0 || mReducingsCount.get() > 0);
        executorService.shutdown();
        return finalResult;
    }

    private class MapperRunnable implements Runnable {

        @Override
        public void run() {
            mMappingsCount.incrementAndGet();
            while (mInputIterator.hasNext()) {
                Pair<MapKey, MapValue> pair;
                synchronized (mMappingsLock) {
                    if (!mInputIterator.hasNext()) break;
                    pair = mInputIterator.next();
                }
                try {
                    mMapper.map(mMapResults, pair.first, pair.second);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (mMappingsCount.decrementAndGet() == 0) {
                mMapResults.finish();
            }
        }
    }
}
