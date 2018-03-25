import java.io.*;
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
  private static final int SUM_MISTAKES_PERCENT = 3;
  private static final int TAX_MISTAKES_PERCENT = 2;
  private static final int CODE_MISTAKES_PERCENT = 1;


  private final static Logger log = Logger.getLogger(DataGenerator.class);

  private String codeInputFileName = "/home/dmitry/IdeaProjects/flsproject/src/main/resources/inn_codes.csv";
  private String outputFileNameFirst = "/home/dmitry/IdeaProjects/flsproject/src/main/resources/output1.csv";
  String outputFileNameSecond = "/home/dmitry/IdeaProjects/flsproject/src/main/resources/output2.csv";


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

  private String[][] addMistakes(String[][] array) {
    int numOfSumMistakes = (SIZE_OF_TABLE * SUM_MISTAKES_PERCENT) / 100 / 4;
    int numOfTaxMistakes = (SIZE_OF_TABLE * TAX_MISTAKES_PERCENT) / 100 / 4;
    int numOfCodeMistakes = (SIZE_OF_TABLE * CODE_MISTAKES_PERCENT) / 100 / 4;
    Random random = new Random();
    for (int i = 0; i < numOfCodeMistakes; i++) {
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

  private void generateData() throws IOException {
    List<String[]> taxCodes = readTaxCodes();
    Random random = new Random();
    try (CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(outputFileNameFirst)));
         CSVWriter writerSecond = new CSVWriter(new BufferedWriter(new FileWriter(outputFileNameSecond)))) {
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
      while (threadsWorkDone < 4) {
        Future<String[][]> futureResult = completionService.take();
        try {
          log.debug("Writing data to csv1");
          String[][] tableValues = futureResult.get();
          writer.writeAll(Arrays.asList(tableValues));
          log.debug("Changing array");
          tableValues = changeColumns(tableValues);
          tableValues = addMistakes(tableValues);
          log.debug("Changing array finished");
          log.debug("Write the second file");
          writerSecond.writeAll(Arrays.asList(tableValues));
          threadsWorkDone++;
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
      }
      executor.shutdown();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  public static void main(String[] args) throws IOException {
    DataGenerator dataGenerator = new DataGenerator();
    dataGenerator.generateData();
//    try (CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter("/home/dmitry/IdeaProjects/flsproject/src/main/resources/output1.csv")))) {
//      writer.writeAll(Arrays.asList(dataGenerator.generationTask(dataGenerator.readTaxCodes(), 1000000)));
//    }
  }

}
