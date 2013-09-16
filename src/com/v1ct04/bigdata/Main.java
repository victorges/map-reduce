package com.v1ct04.bigdata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Main {

    public static final List<Pair<Integer,String>> FILES = Arrays.asList(
        Pair.of(1, "in1.txt"),
        Pair.of(2, "in1.txt"),
        Pair.of(3, "in1.txt"),
        Pair.of(4, "in1.txt"),
        Pair.of(2, "in1.txt"),
        Pair.of(3, "in1.txt"),
        Pair.of(4, "in1.txt"),
        Pair.of(2, "in1.txt"),
        Pair.of(3, "in1.txt"),
        Pair.of(4, "in1.txt"),
        Pair.of(2, "in1.txt"),
        Pair.of(3, "in1.txt"),
        Pair.of(4, "in1.txt"),
        Pair.of(2, "ni1.txt"),
        Pair.of(3, "in1.txt"),
        Pair.of(4, "in1.txt"),
        Pair.of(2, "in1.txt"),
        Pair.of(3, "in1.txt"),
        Pair.of(4, "in1.txt"),
        Pair.of(4, "in1.txt"));

    public static void main(String[] args) {
        MapReducer<Integer, String, String, Integer, Integer> mapReducer =
            new MapReducer<>(new FileMapper(), new WordCounterReducer());

        System.out.println("started");
        long startTime = System.currentTimeMillis();
        Map<String, Integer> result = mapReducer.mapReduce(FILES);
        long finishTime = System.currentTimeMillis();

        List<Map.Entry<String, Integer>> resultList = new ArrayList<>(result.entrySet());
        Collections.sort(resultList, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o1.getValue() - o2.getValue();
            }
        });
        for (Map.Entry<String, Integer> entry : resultList) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
        System.out.println("finished in " + (finishTime - startTime) + "ms");
    }

    private static class FileMapper implements Mapper<Integer, String, String, Integer> {

        @Override
        public void map(Yielder <Pair<String, Integer>> yielder, Integer id, String filename)
                throws IOException {
            Map<String, Integer> wordCount = new HashMap<>();
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(filename))) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    for (String word : line.split(" ")) {
                        word = word.replaceAll("[^A-z]", "").toLowerCase();
                        if (!word.isEmpty()) {
                            if (!wordCount.containsKey(word)) wordCount.put(word, 1);
                            else wordCount.put(word, wordCount.get(word) + 1);
                        }
                    }
                }
            }
            for (Map.Entry<String, Integer> word : wordCount.entrySet()) {
                yielder.yield(Pair.of(word.getKey(), word.getValue()));
            }
        }
    }

    private static class WordCounterReducer implements Reducer<String, Integer, Integer> {

        @Override
        public Integer reduce(String s, Iterable<Integer> value) {
            int count = 0;
            for (Integer singleCount : value) count += singleCount;
            return count;
        }
    }
}
