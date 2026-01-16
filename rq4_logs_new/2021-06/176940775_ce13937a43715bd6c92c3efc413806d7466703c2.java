//package net.awtg.comreg.utils;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.List;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Temporary utilities for comparing rasters made from ASC and TXT files.<br>
 * Execute main method only. Not for usages in other classes.
 *
 * @author Evgeni Bokhanov
 */
public class AscTempHelper { // TODO do not use delimiter \\

    // Dimension * images count
    private static final double COVERAGE_MAP_PIXELS_COUNT = 1280 * 1600 * 1024;
//    private static final double COVERAGE_MAP_PIXELS_COUNT = 1280 * 1600 * 2;

    public static void main(String[] args) throws IOException, CsvException {
        Map<String, String> levelsMap = new HashMap<>();
        levelsMap.put("90_45_6_255", "Very Good");
        levelsMap.put("176_88_11_255", "Good");
        levelsMap.put("205_148_99_255", "Fair");
        levelsMap.put("229_200_175_255", "Fringe");
        levelsMap.put("255_255_255_255", "No coverage");
        levelsMap.put("0_0_0_0", "Transparent");

        // note that root paths must end with slash '\\' or '/'
//        String txtRootPath = "H:\\RASTER_5\\0b392f470e5e4b55955e6f7ee85745d0\\";
//        String ascRootPath = "H:\\RASTER_6\\428ba7e004934ab38932762a4a8c2e1b\\"; // TODO change paths to ASC paths
        String txtRootPath = "comreg/ruster1/";
        String ascRootPath = "comreg/ruster3/"; // TODO change paths to ASC paths
        String comparedCsfFile = "comreg/compare/";
        if (new File(txtRootPath + "csv").mkdirs()) {
            System.out.println("Folder creation error!");
        }
        if (new File(ascRootPath + "csv").mkdirs()) {
            System.out.println("Folder creation error!");
        }
        if (new File(txtRootPath + "compare").mkdirs()) {
            System.out.println("Folder creation error!");
        }
//
//        compareOneRaster(txtRootPath, levelsMap);
//        compareOneRaster(ascRootPath, levelsMap);
//        compareTwoRasters(comparedCsfFile, txtRootPath, ascRootPath, ascRootPath, levelsMap);
//
//        Long startTime = System.currentTimeMillis();
//        createReport();
//        Long endTime = System.currentTimeMillis();
//        System.out.println(endTime-startTime);
        String txtRootPath1 = "/home/dmtech/comreg/ruster1/";
        String ascRootPath1 = "/home/dmtech/comreg/ruster3/"; // TODO change paths to ASC paths

        ReportHelper reportHelper = new ReportHelper();
        reportHelper.createReport(txtRootPath1, ascRootPath1, "trtrtrt" );
    }

    /**
     * Compare two raster sets by finding differ pixels.
     * Create a report file with all overalls.
     *
     * @param rootPath1 Root path of first raster set
     * @param rootPath2 Root path of second raster set
     * @param csvPath Output CSV file path
     * @param levelsMap Coverage levels
     * @throws IOException If some folders of files not found or not have access
     */
    private static void compareTwoRasters(String comparedCsfFile,String rootPath1, String rootPath2, String csvPath,
                                          Map<String, String> levelsMap) throws IOException {
//        Map<String[], Integer> eir2g = findDifference(rootPath1 + "eir_2g\\0_0", rootPath2 + "eir_2g\\0_0", rootPath1 + "compare\\eir_2g.csv", levelsMap);
//        Map<String[], Integer> eir3g = findDifference(rootPath1 + "eir_3g\\0_0", rootPath2 + "eir_3g\\0_0", rootPath1 + "compare\\eir_3g.csv", levelsMap);
//        Map<String[], Integer> eir4g = findDifference(rootPath1 + "eir_4g\\0_0", rootPath2 + "eir_4g\\0_0", rootPath1 + "compare\\eir_4g.csv", levelsMap);
//        Map<String[], Integer> three2g = findDifference(rootPath1 + "three_2g\\0_0", rootPath2 +"three_2g\\0_0", rootPath1 + "compare\\three_2g.csv", levelsMap);
//        Map<String[], Integer> three3g = findDifference(rootPath1 + "three_3g\\0_0", rootPath2 +"three_3g\\0_0", rootPath1 + "compare\\three_3g.csv", levelsMap);
//        Map<String[], Integer> three4g = findDifference(rootPath1 + "three_4g\\0_0", rootPath2 +"three_4g\\0_0", rootPath1 + "compare\\three_4g.csv", levelsMap);
//        Map<String[], Integer> vodafone2g = findDifference(rootPath1 + "vodafone_2g\\0_0", rootPath2 +"vodafone_2g\\0_0", rootPath1 + "compare\\vodafone_2g.csv", levelsMap);
//        Map<String[], Integer> vodafone3g = findDifference(rootPath1 + "vodafone_3g\\0_0", rootPath2 +"vodafone_3g\\0_0", rootPath1 + "compare\\vodafone_3g.csv", levelsMap);
//        Map<String[], Integer> vodafone4g = findDifference(rootPath1 + "vodafone_4g\\0_0", rootPath2 +"vodafone_4g\\0_0", rootPath1 + "compare\\vodafone_4g.csv", levelsMap);
        Map<String[], Integer> eir2g = findDifference(rootPath1 + "eir_2g", rootPath2 + "eir_2g", comparedCsfFile + "eir_2g.csv", levelsMap);
        Map<String[], Integer> eir3g = findDifference(rootPath1 + "eir_3g", rootPath2 + "eir_3g", comparedCsfFile + "eir_3g.csv", levelsMap);
        Map<String[], Integer> eir4g = findDifference(rootPath1 + "eir_4g", rootPath2 + "eir_4g", comparedCsfFile + "eir_4g.csv", levelsMap);
        Map<String[], Integer> three2g = findDifference(rootPath1 + "three_2g", rootPath2 +"three_2g", comparedCsfFile + "three_2g.csv", levelsMap);
        Map<String[], Integer> three3g = findDifference(rootPath1 + "three_3g", rootPath2 +"three_3g", comparedCsfFile + "three_3g.csv", levelsMap);
        Map<String[], Integer> three4g = findDifference(rootPath1 + "three_4g", rootPath2 +"three_4g", comparedCsfFile + "three_4g.csv", levelsMap);
        Map<String[], Integer> vodafone2g = findDifference(rootPath1 + "vodafone_2g", rootPath2 +"vodafone_2g", comparedCsfFile + "vodafone_2g.csv", levelsMap);
        Map<String[], Integer> vodafone3g = findDifference(rootPath1 + "vodafone_3g", rootPath2 +"vodafone_3g", comparedCsfFile + "vodafone_3g.csv", levelsMap);
        Map<String[], Integer> vodafone4g = findDifference(rootPath1 + "vodafone_4g", rootPath2 +"vodafone_4g", comparedCsfFile + "vodafone_4g.csv", levelsMap);
//        // Create CSV report
//        CSVWriter csv = new CSVWriter(new FileWriter(new File(csvPath + "report.csv")));
//        csv.writeNext(new String[]{"TXT coverage level (old)", "ASC coverage level (new)", "Eir 2G", "Eir 3G",
//                "Eir 4G", "Three 2G", "Three 3G", "Three 4G", "Vodafone 2G", "Vodafone 3G", "Vodafone 4G"});
//        String[][] comparisons = {
//                {"Fair", "Fringe"},
//                {"Fair", "Good"},
//                {"Fair", "No coverage"},
//                {"Fair", "Transparent"},
//                {"Fair", "Very Good"},
//                {"Fringe", "Fair"},
//                {"Fringe", "Good"},
//                {"Fringe", "No coverage"},
//                {"Fringe", "Transparent"},
//                {"Fringe", "Very Good"},
//                {"Good", "Fair"},
//                {"Good", "Fringe"},
//                {"Good", "No coverage"},
//                {"Good", "Transparent"},
//                {"Good", "Very Good"},
//                {"No coverage", "Fair"},
//                {"No coverage", "Fringe"},
//                {"No coverage", "Good"},
//                {"No coverage", "Transparent"},
//                {"No coverage", "Very Good"},
//                {"Transparent", "Fair"},
//                {"Transparent", "Fringe"},
//                {"Transparent", "Good"},
//                {"Transparent", "No coverage"},
//                {"Transparent", "Very Good"},
//                {"Very Good", "Fair"},
//                {"Very Good", "Fringe"},
//                {"Very Good", "Good"},
//                {"Very Good", "No coverage"},
//                {"Very Good", "Transparent"}
//        };
//
//
//        Arrays.stream(comparisons).forEach(comparison -> {
//            String[] line = {
//                    comparison[0],
//                    comparison[1],
//                    String.valueOf(((double) Optional.ofNullable(eir2g.get(comparison)).orElse(0)) * 100.0 / COVERAGE_MAP_PIXELS_COUNT),
//                    String.valueOf(((double) Optional.ofNullable(eir3g.get(comparison)).orElse(0)) * 100.0 / COVERAGE_MAP_PIXELS_COUNT),
//                    String.valueOf(((double) Optional.ofNullable(eir4g.get(comparison)).orElse(0)) * 100.0 / COVERAGE_MAP_PIXELS_COUNT),
//                    String.valueOf(((double) Optional.ofNullable(three2g.get(comparison)).orElse(0)) * 100.0 / COVERAGE_MAP_PIXELS_COUNT),
//                    String.valueOf(((double) Optional.ofNullable(three3g.get(comparison)).orElse(0)) * 100.0 / COVERAGE_MAP_PIXELS_COUNT),
//                    String.valueOf(((double) Optional.ofNullable(three4g.get(comparison)).orElse(0)) * 100.0 / COVERAGE_MAP_PIXELS_COUNT),
//                    String.valueOf(((double) Optional.ofNullable(vodafone2g.get(comparison)).orElse(0)) * 100.0 / COVERAGE_MAP_PIXELS_COUNT),
//                    String.valueOf(((double) Optional.ofNullable(vodafone3g.get(comparison)).orElse(0)) * 100.0 / COVERAGE_MAP_PIXELS_COUNT),
//                    String.valueOf(((double) Optional.ofNullable(vodafone4g.get(comparison)).orElse(0)) * 100.0 / COVERAGE_MAP_PIXELS_COUNT)
//            };
//            csv.writeNext(line);
//        });
//        csv.writeNext(new String[]{"", "Total pixels count", String.valueOf(COVERAGE_MAP_PIXELS_COUNT)});
//
//        csv.close();
    }

    /**
     * Count pixels by coverage levels for given raster set.
     *
     * @param rootPath Root path of raster set
     * @param levelsMap Coverage levels
     * @throws IOException If some folders of files not found or not have access
     */
    private static void compareOneRaster(String rootPath, Map<String, String> levelsMap) throws IOException {
//        calculateLevelPixels(rootPath + "eir_2g\\0_0", rootPath + "csv\\eir_2g.csv", levelsMap);
//        calculateLevelPixels(rootPath + "eir_3g\\0_0", rootPath + "csv\\eir_3g.csv", levelsMap);
//        calculateLevelPixels(rootPath + "eir_4g\\0_0", rootPath + "csv\\eir_4g.csv", levelsMap);
//        calculateLevelPixels(rootPath + "three_2g\\0_0", rootPath + "csv\\three_2g.csv", levelsMap);
//        calculateLevelPixels(rootPath + "three_3g\\0_0", rootPath + "csv\\three_3g.csv", levelsMap);
//        calculateLevelPixels(rootPath + "three_4g\\0_0", rootPath + "csv\\three_4g.csv", levelsMap);
//        calculateLevelPixels(rootPath + "vodafone_2g\\0_0", rootPath + "csv\\vodafone_2g.csv", levelsMap);
//        calculateLevelPixels(rootPath + "vodafone_3g\\0_0", rootPath + "csv\\vodafone_3g.csv", levelsMap);
//        calculateLevelPixels(rootPath + "vodafone_4g\\0_0", rootPath + "csv\\vodafone_4g.csv", levelsMap);
        calculateLevelPixels(rootPath + "eir_2g", rootPath + "csv/eir_2g.csv", levelsMap);
        calculateLevelPixels(rootPath + "eir_3g", rootPath + "csv/eir_3g.csv", levelsMap);
        calculateLevelPixels(rootPath + "eir_4g", rootPath + "csv/eir_4g.csv", levelsMap);
        calculateLevelPixels(rootPath + "three_2g", rootPath + "csv/three_2g.csv", levelsMap);
        calculateLevelPixels(rootPath + "three_3g", rootPath + "csv/three_3g.csv", levelsMap);
        calculateLevelPixels(rootPath + "three_4g", rootPath + "csv/three_4g.csv", levelsMap);
        calculateLevelPixels(rootPath + "vodafone_2g", rootPath + "csv/vodafone_2g.csv", levelsMap);
        calculateLevelPixels(rootPath + "vodafone_3g", rootPath + "csv/vodafone_3g.csv", levelsMap);
        calculateLevelPixels(rootPath + "vodafone_4g", rootPath + "csv/vodafone_4g.csv", levelsMap);
    }

    /**
     * Compare two raster by finding differ pixels.
     * Return Map of comparing levels and appropriate differing pixels count.<br>
     * E.g. key = ['Good', 'Fair'], value = 100
     *
     * @param folderPath1 Input folder path of first raster
     * @param folderPath2 Input folder path of second raster
     * @param csvPath Output CSV file path
     * @param levelsMap Coverage levels
     * @return Map of differences
     * @throws IOException If some folders of files not found or not have access
     */
    private static Map<String[], Integer> findDifference(String folderPath1, String folderPath2, String csvPath,
                                                         Map<String, String> levelsMap) throws IOException {
        System.out.println("Started: ");
        FileWriter fw = new FileWriter(new File(csvPath));
        CSVWriter csv = new CSVWriter(fw);
        List<Map<String, Integer>> overall = new ArrayList<>();
        List<File> files = new ArrayList<>(Arrays.asList(Objects.requireNonNull(new File(folderPath1).listFiles(),
                "Cant access folder")))
                .stream()
                .filter(f -> f.getName().endsWith("png")) // filter only PNG files
                .sorted(Comparator.comparing(e -> Integer.valueOf(e.getName().split("\\.")[0]))) // sort numerically
                .collect(Collectors.toList());
        for (File file: files) {
            Map<String, Integer> map = new HashMap<>();
            BufferedImage image1;
            BufferedImage image2;
            try {
                image1 = ImageIO.read(file);
//                image2 = ImageIO.read(new File(folderPath2 + "\\" + file.getName()));
                image2 = ImageIO.read(new File(folderPath2 + "/" + file.getName()));
                for (int y = 0; y < image1.getHeight(); y++) {
                    for (int x = 0; x < image1.getWidth(); x++) {
                        int color1 = image1.getRGB(x, y);
                        int t1 = new Color(image1.getRGB(x, y), true).getAlpha();
                        int r1 = (color1 & 0x00ff0000) >> 16;
                        int g1 = (color1 & 0x0000ff00) >> 8;
                        int b1 =  color1 & 0x000000ff;
                        int color2 = image2.getRGB(x, y);
                        int t2 = new Color(image2.getRGB(x, y), true).getAlpha();
                        int r2 = (color2 & 0x00ff0000) >> 16;
                        int g2 = (color2 & 0x0000ff00) >> 8;
                        int b2 =  color2 & 0x000000ff;
                        if (color1 != color2 || t1 != t2) {
                            String colorStr = r1 + "_" + g1 + "_" + b1 + "_" + t1 + "#" + r2 + "_" + g2 + "_" + b2 + "_"
                                    + t2;
                            plus(map, colorStr);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("\nFile: " + file.getName());
            overall.add(map);
            csv.writeNext(new String[]{file.getName()});
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String[] levels = colorsToLevels(entry.getKey(), levelsMap);
                String convertedKey = levels[0] + " => " + levels[1];
                csv.writeNext(new String[]{convertedKey, entry.getValue().toString()});
                System.out.println(convertedKey + " : " + entry.getValue());
            }
        }
        // summarize file data to raster
        Map<String, Integer> mapOverall = calculateOverall(overall);
        csv.writeNext(new String[]{"Overall"});
        System.out.println("\nOverall: ");
        Map<String[], Integer> overallDifferences = new HashMap<>();
        for (Map.Entry<String, Integer> entry : mapOverall.entrySet()) {
            String[] levels = colorsToLevels(entry.getKey(), levelsMap);
            String convertedKey = levels[0] + " => " + levels[1];
            Integer value = entry.getValue();
            csv.writeNext(new String[]{convertedKey, value.toString()});
            System.out.println(convertedKey + " : " + value);
            overallDifferences.put(levels, value);
        }
        csv.close();

        return overallDifferences;
    }

    /**
     * Counts number of pixels for given levels.<br>
     * Iterates over all PNG images in given folder.<br>
     * For each image iterates over all pixels.<br>
     * Converts pixel to coverage level and counts it.<br>
     * Writes results for each image and summarized values to give CSV file.<br>
     * Creates CSV file if it not exist, otherwise overwrites it.
     *
     * @param folderPath Input folder path
     * @param csvPath Output CSV file path
     * @param levelsMap Map of coverage levels and its colors
     * @throws IOException If some folders of files not found or not have access
     */
    private static void calculateLevelPixels(String folderPath, String csvPath, Map<String, String> levelsMap)
            throws IOException {
        System.out.println(folderPath);
        System.out.println("Started: ");
        FileWriter fw = new FileWriter(new File(csvPath));
        CSVWriter csv = new CSVWriter(fw);
        List<Map<String, Integer>> overall = new ArrayList<>();
        List<File> files = new ArrayList<>(Arrays.asList(Objects.requireNonNull(new File(folderPath).listFiles(),
                "Cant access folder")))
                .stream()
                .filter(f -> f.getName().endsWith("png")) // filter only PNG files
//                .sorted(Comparator.comparing(e -> Integer.valueOf(e.getName().split("\\.")[0]))) // sort numerically
                .sorted(Comparator.comparing(e -> Integer.valueOf(e.getName().split("\\.")[0]))) // sort numerically
                .collect(Collectors.toList());
        for (File file: files) {
            Map<String, Integer> map = new HashMap<>();
            BufferedImage image;
            try {
                image = ImageIO.read(file);
                for (int y = 0; y < image.getHeight(); y++) {
                    for (int x = 0; x < image.getWidth(); x++) {
                        int color = image.getRGB(x, y);
                        int t = new Color(image.getRGB(x, y), true).getAlpha();
                        int r = (color & 0x00ff0000) >> 16;
                        int g = (color & 0x0000ff00) >> 8;
                        int b =  color & 0x000000ff;
                        plus(map, r + "_" + g + "_" + b + "_" +	t);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("\nFile: " + file.getName());
            overall.add(map);
            csv.writeNext(new String[]{file.getName()});
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                csv.writeNext(new String[]{levelsMap.get(entry.getKey()), entry.getValue().toString()});
                System.out.println(levelsMap.get(entry.getKey()) + " : " + entry.getValue());
            }
        }
        // summarize file data to raster
        Map<String, Integer> mapOverall = calculateOverall(overall);
        csv.writeNext(new String[]{"Overall"});
        System.out.println("\nOverall: ");
        for (Map.Entry<String, Integer> entry : mapOverall.entrySet()) {
            csv.writeNext(new String[]{levelsMap.get(entry.getKey()), entry.getValue().toString()});
            System.out.println(levelsMap.get(entry.getKey()) + " : " + entry.getValue());
        }
        csv.close();
    }

    /**
     * Increment value in the map by for given key.<br>
     * If map doesn't contain key, adds it to the map with 0 value.
     *
     * @param map Map
     * @param key Key
     */
    private static void plus(Map<String, Integer> map, String key) {
        if (!map.containsKey(key)) { // init with 0 if not contains
            map.put(key, 0);
        }
        Integer counter = map.get(key);
        counter = counter + 1;
        map.put(key, counter);
    }

    /**
     * Summarize values from list of map to one map.
     *
     * @param overall List of maps to summarize
     * @return Map
     */
    private static Map<String, Integer> calculateOverall(List<Map<String, Integer>> overall) {
        Map<String, Integer> out = new HashMap<>();
        for (Map<String, Integer> map: overall) {
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                if (!out.containsKey(entry.getKey())) { // init with 0 if not contains
                    out.put(entry.getKey(), 0);
                }
                Integer now = out.get(entry.getKey());
                now = now + entry.getValue();
                out.put(entry.getKey(), now);
            }
        }
        return out;
    }

    /**
     * Convert two colors string to two levels string array.<br>
     * E.g. '176_88_11_255#0_0_0_0' => ['Good', 'Transparent']
     *
     * @param colorStr Color string, e.g. '176_88_11_255#0_0_0_0'
     * @param levelsMap Coverage level map
     * @return Readable string array
     */
    private static String[] colorsToLevels(String colorStr, Map<String, String> levelsMap) {
        String[] arr = colorStr.split("#");
        arr[0] = Optional.ofNullable(levelsMap.get(arr[0])).orElse("No coverage");
        arr[1] = Optional.ofNullable(levelsMap.get(arr[1])).orElse("No coverage");
        return arr;
    }

    private static void createReport() throws IOException, CsvException {
        Map<Integer, Map<Integer, Integer>> parsedCsvMap = new HashMap<>();
        parsedCsvMap.put(3, parseCsv("eir_2g.csv"));
        parsedCsvMap.put(4, parseCsv("eir_3g.csv"));
        parsedCsvMap.put(5, parseCsv("eir_4g.csv"));
        parsedCsvMap.put(6, parseCsv("three_2g.csv"));
        parsedCsvMap.put(7, parseCsv("three_3g.csv"));
        parsedCsvMap.put(8, parseCsv("three_4g.csv"));
        parsedCsvMap.put(9, parseCsv("vodafone_2g.csv"));
        parsedCsvMap.put(10, parseCsv("vodafone_3g.csv"));
        parsedCsvMap.put(11, parseCsv("vodafone_4g.csv"));
        fillColumns(parsedCsvMap);

    }

    private static Map<Integer, Integer> parseCsv(String fileName) throws IOException, CsvException {
        String path ="comreg/compare/" + fileName;
        CSVReader reader = new CSVReader(new FileReader(path));
        Map<String,Integer> comparisons = new HashMap<>();
        System.out.println(getIndex(path));
        reader.skip(getIndex(path));
        String [] nextLine;
        while ((nextLine = reader.readNext()) != null) {

            comparisons.put(nextLine[0], Integer.valueOf(nextLine[1]));
        }

        Map<String, Integer> comparisonsLevels = new HashMap<>();
        comparisonsLevels.put("Fair => Fringe",3);
        comparisonsLevels.put("Fair => Good",4);
        comparisonsLevels.put("Fair => No coverage",5);
        comparisonsLevels.put("Fair => Transparent",6);
        comparisonsLevels.put("Fair => Very Good",7);
        comparisonsLevels.put("Fringe => Fair",8);
        comparisonsLevels.put("Fringe => Good",9);
        comparisonsLevels.put("Fringe => No coverage",10);
        comparisonsLevels.put("Fringe => Transparent",11);
        comparisonsLevels.put("Fringe => Very Good",12);
        comparisonsLevels.put("Good => Fair",13);
        comparisonsLevels.put("Good => Fringe",14);
        comparisonsLevels.put("Good => No coverage",15);
        comparisonsLevels.put("Good => Transparent",16);
        comparisonsLevels.put("Good => Very Good",17);
        comparisonsLevels.put("No coverage => Fair",18);
        comparisonsLevels.put("No coverage => Fringe",19);
        comparisonsLevels.put("No coverage => Good",20);
        comparisonsLevels.put("No coverage => Transparent",21);
        comparisonsLevels.put("No coverage => Very Good",22);
        comparisonsLevels.put("Transparent => Fair",23);
        comparisonsLevels.put("Transparent => Fringe",24);
        comparisonsLevels.put("Transparent => Good",25);
        comparisonsLevels.put("Transparent => No coverage",26);
        comparisonsLevels.put("Transparent => Very Good",27);
        comparisonsLevels.put("Very Good => Fair",28);
        comparisonsLevels.put("Very Good => Fringe",29);
        comparisonsLevels.put("Very Good => Good",30);
        comparisonsLevels.put("Very Good => No coverage",31);
        comparisonsLevels.put("Very Good => Transparent",32);

    Map<Integer, Integer> parametersLocation = new HashMap<>();
        for(Map.Entry<String,Integer> entry : comparisons.entrySet()){
            parametersLocation.put(comparisonsLevels.get(entry.getKey()),entry.getValue());
        }
    return parametersLocation;

    }
    private static int getIndex (String path) throws IOException, CsvException {
        CSVReader reader = new CSVReader(new FileReader(path));
        List<String[]> rows = reader.readAll();
        for(String[] column: rows) {
            if (column[0].equals("Overall")) {
                return rows.indexOf(column) + 1;
            }
        }
        return 0;
    }

    public static void fillColumns(Map<Integer, Map<Integer, Integer>> parsedCsv) throws IOException {
        FileInputStream inputStream = new FileInputStream(new File("comreg/Report.xlsx"));
        XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
        XSSFSheet pixelSheet = workbook.getSheetAt(1);
        XSSFSheet percentSheet = workbook.getSheetAt(0);

        Row pixelsCountRow = percentSheet.getRow(33);
        //Fill pixel count cells
        for(int i = 3; i <= 11; i++){
            Cell pixelsCountCell = pixelsCountRow.getCell(i);
            pixelsCountCell.setCellValue(COVERAGE_MAP_PIXELS_COUNT);
        }

        for (Map.Entry<Integer, Map<Integer, Integer>> csvMap : parsedCsv.entrySet() ){

            for(Map.Entry<Integer,Integer> entry : csvMap.getValue().entrySet()) {
            Row pixelRow = pixelSheet.getRow(entry.getKey());
            Cell pixelCell = pixelRow.getCell(csvMap.getKey());
            pixelCell.setCellValue(entry.getValue());

            Row percentRow = percentSheet.getRow(entry.getKey());
            Cell percentCell = percentRow.getCell(csvMap.getKey());
            percentCell.setCellValue(entry.getValue() / COVERAGE_MAP_PIXELS_COUNT);
            }
        }
        try {
            FileOutputStream out = new FileOutputStream(new File("comreg/Result22.xlsx"));
            workbook.write(out);
            out.close();
            System.out.println("Report written successfully on disk.");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


}
