package com.akka;

import java.math.BigDecimal;

public class App {
    public static void main(String... args) throws Exception {
        String inPath = args[0];
        String outPath1 = args[1];
        String outPath2 = args[2];
        String outPath3 = args[3];
        System.out.println(inPath);
        System.out.println(outPath1);
        System.out.println(outPath2);
        System.out.println(outPath3);

        int MAXIMUM_DISTINCT_IDS = 1000; //количество уникальных записей

        StreamManager.collectToFile("sys1", inPath, outPath1, new BigDecimalTypeResolver(), MAXIMUM_DISTINCT_IDS);
        StreamManager.collectToFileAndConsole("sys2", inPath, outPath2, new BigDecimalTypeResolver(), MAXIMUM_DISTINCT_IDS);
        StreamManager.collectToFile("sys3", inPath, outPath3, new BigDecimalTypeResolver());
    }

    static class BigDecimalTypeResolver implements StreamManager.TypeAndOperationResolver<BigDecimal> {
        @Override
        public BigDecimal valueOf(String s) {
            return new BigDecimal(s);
        }

        @Override
        public BigDecimal compute(BigDecimal left, BigDecimal right) {
            return left.add(right);
        }

        @Override
        public BigDecimal empty() {
            return BigDecimal.ZERO;
        }
    }
}
