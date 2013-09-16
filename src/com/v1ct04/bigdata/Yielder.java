package com.v1ct04.bigdata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Yielder<Type> implements Iterable<Type> {

    private List<Type> mResults = new ArrayList<>();
    private boolean mFinished;

    @Override
    public Iterator<Type> iterator() {
        return new YielderIterator();
    }

    public void yield(Type type) {
        synchronized (this) {
            if (mFinished) throw new IllegalStateException("Yielder already finished");
            mResults.add(type);
            notifyAll();
        }
    }

    public void finish() {
        synchronized (this) {
            mFinished = true;
            notifyAll();
        }
    }

    private class YielderIterator implements Iterator<Type> {

        private int mCount = 0;

        @Override
        public boolean hasNext() {
            if (mCount < mResults.size()) return true;
            if (mFinished) return false;
            synchronized (Yielder.this) {
                if (mCount < mResults.size()) return true;
                if (mFinished) return false;
                try {
                    Yielder.this.wait();
                } catch (InterruptedException e) {
                    return false;
                }
            }
            return mCount < mResults.size();
        }

        @Override
        public Type next() {
            synchronized (Yielder.this) {
                if (hasNext()) {
                    return mResults.get(mCount++);
                }
            }
            throw new IllegalArgumentException("No next element left");
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("No removes allowed from Yielder iterator");
        }
    }
}
