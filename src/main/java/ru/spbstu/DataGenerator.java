package ru.spbstu;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

import com.opencsv.*;
import org.apache.log4j.Logger;


public class DataGenerator {

    private static final int RANDOM_PART_INN_UPPER_BOUND = 1000000;
    private static final int RANDOM_PART_INN_LOWER_BOUND = 100000;
    private static final int RANDOM_PART_KPP_UPPER_BOUND = 100000;
    private static final int RANDOM_PART_KPP_LOWER_BOUND = 10000;
    private static final int MINIMUM_SUM = 10;
    private static final int MAXIMUM_SUM = 10000000;
    private static final double TAX_PERCENT = 0.18;
    private static final int SIZE_OF_TABLE = 1000000;
    private static final int SUM_MISTAKES_PERCENT = 5;
    private static final int TAX_MISTAKES_PERCENT = 2;
    private static final int CODE_MISTAKES_PERCENT = 1;
    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDrive";
    private static final String SALES_TABLE_NAME = "SALES";
    private static final String PURCHASES_TABLE_NAME = "PURCHASES";
    public static final String DEFAULT_CONNECTION_URL = "jdbc:hive2://localhost:10000/;ssl=false";

    private final static Logger log = Logger.getLogger(DataGenerator.class);

    private String codeInputFileName = "../../../resources/inn_codes.csv";


    public List<String[]> readTaxCodes() throws IOException {
        CSVParser csvParser = new CSVParserBuilder().withSeparator(',').build();
        try (CSVReader reader = new CSVReaderBuilder(
                new BufferedReader(
                        new FileReader(codeInputFileName))).withCSVParser(csvParser).build()) {
            return reader.readAll();
        }
    }

    private String generateRandomPartOfCode(String regionCode, Random random, int upperBound, int lowerBound) {
        String randomPart = Integer.toString(random.nextInt(upperBound - lowerBound) + lowerBound);
        return regionCode + randomPart;
    }

    private double generateRandomSum(Random random) {
        return random.nextFloat() * (MAXIMUM_SUM - MINIMUM_SUM) + MINIMUM_SUM;
    }


    private String[][] generationTask(List<String[]> taxCodes, int size) throws IOException {
        String[][] result = new String[size][];
        for (int i = 0; i < size; i++) {
            result[i] = new String[6];
            Random random = new Random();
            String regionSellerCode = taxCodes.get(random.nextInt(taxCodes.size()))[0];
            result[i][0] = generateRandomPartOfCode(regionSellerCode, random, RANDOM_PART_INN_UPPER_BOUND, RANDOM_PART_INN_LOWER_BOUND);
            result[i][1] = generateRandomPartOfCode(regionSellerCode, random, RANDOM_PART_KPP_UPPER_BOUND, RANDOM_PART_KPP_LOWER_BOUND);
            String regionBuyerCode = taxCodes.get(random.nextInt(taxCodes.size()))[0];
            result[i][2] = generateRandomPartOfCode(regionBuyerCode, random, RANDOM_PART_INN_UPPER_BOUND, RANDOM_PART_INN_LOWER_BOUND);
            result[i][3] = generateRandomPartOfCode(regionBuyerCode, random, RANDOM_PART_KPP_UPPER_BOUND, RANDOM_PART_KPP_LOWER_BOUND);
            double sum = generateRandomSum(random);
            result[i][4] = Double.toString(sum);
            result[i][5] = Double.toString(sum * TAX_PERCENT);
        }
        return result;
    }

    private String[][] changeColumns(String[][] array) {
        for (int i = 0; i < array.length; i++) {
            String tempFirst = array[i][0];
            String tempSecond = array[i][1];
            array[i][0] = array[i][2];
            array[i][1] = array[i][3];
            array[i][2] = tempFirst;
            array[i][3] = tempSecond;
        }
        return array;
    }

    private String makeInsertSqlQueryString(String tableName, String[][] values) {
        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append(" INSERT INTO TABLE ").append(tableName).append(" VALUES (");
        for (String[] row : values) {
            for (String value : row) {
                sqlQuery.append(value).append(",");
            }
            sqlQuery.delete(sqlQuery.length() - 1, sqlQuery.length());
        }
        sqlQuery.append(")");
        return sqlQuery.toString();
    }

    private String[][] addMistakes(String[][] array) {
        int numOfSumMistakes = (SIZE_OF_TABLE * SUM_MISTAKES_PERCENT) / 100 / 4;
        int numOfTaxMistakes = (SIZE_OF_TABLE * TAX_MISTAKES_PERCENT) / 100 / 4;
        int numOfCodeMistakes = (SIZE_OF_TABLE * CODE_MISTAKES_PERCENT) / 100 / 4;
        Random random = new Random();
        for (int i = 0; i < numOfSumMistakes; i++) {
            array[random.nextInt(array.length)][random.nextInt(4)] =
                    generateRandomPartOfCode("4300", random, RANDOM_PART_INN_UPPER_BOUND, RANDOM_PART_INN_LOWER_BOUND);
        }
        for (int i = 0; i < numOfTaxMistakes; i++) {
            array[random.nextInt(array.length)][4] = String.valueOf(generateRandomSum(random));
        }
        for (int i = 0; i < numOfCodeMistakes; i++) {
            array[random.nextInt(array.length)][5] = String.valueOf(generateRandomSum(random) * TAX_PERCENT);
        }
        return array;
    }

    private void generateData(String connectionUrl) throws IOException, InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
        List<String[]> taxCodes = readTaxCodes();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CompletionService<String[][]> completionService = new ExecutorCompletionService<>(executor);
        for (int i = 0; i < 4; i++) {
            completionService.submit(() -> {
                log.debug("THREAD " + Thread.currentThread().getName() + " started generationTask");
                String[][] strings = generationTask(taxCodes, SIZE_OF_TABLE / 4);
                log.debug("THREAD " + Thread.currentThread().getName() + " executed");
                return strings;
            });
        }
        int threadsWorkDone = 0;
        Class.forName(JDBC_DRIVER_NAME);
        Connection con = DriverManager.getConnection(connectionUrl, "hdfs", "");
        Statement statement = con.createStatement();
        PreparedStatement prep = con.prepareStatement("DROP TABLE IF EXISTS ?");
        prep.addBatch(SALES_TABLE_NAME);
        prep.execute();
        prep.addBatch(PURCHASES_TABLE_NAME);
        prep.execute();
        statement.execute("CREATE TABLE "
                + SALES_TABLE_NAME
                + " (seller_inn INT, seller_kpp INT, buyer_inn INT, buyer_kpp INT, sum INT, tax DOUBLE)");
        statement.execute("CREATE TABLE "
                + PURCHASES_TABLE_NAME
                + " (buyer_inn INT, buyer_kpp INT, seller_inn INT, seller_kpp INT, sum INT, tax DOUBLE)");
        while (threadsWorkDone < 4) {
            Future<String[][]> futureResult = completionService.take();
            log.debug("Write the first table");
            String[][] tableValues = futureResult.get();
            statement.execute(makeInsertSqlQueryString(SALES_TABLE_NAME, tableValues));
            log.debug("Changing array");
            tableValues = changeColumns(tableValues);
            tableValues = addMistakes(tableValues);
            log.debug("Changing array finished");
            statement.execute(makeInsertSqlQueryString(SALES_TABLE_NAME, tableValues));
            log.debug("Write the second table");
            threadsWorkDone++;
        }
        executor.shutdown();
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.generateData(DEFAULT_CONNECTION_URL);
    }

}
