package com.v1ct04.bigdata;

public class Pair<FirstType, SecondType> {

    public final FirstType first;
    public final SecondType second;

    private Pair(FirstType first, SecondType second) {
        this.first = first;
        this.second = second;
    }

    public static <First, Second> Pair<First, Second> of(First first, Second second) {
        return new Pair<>(first, second);
    }
}
