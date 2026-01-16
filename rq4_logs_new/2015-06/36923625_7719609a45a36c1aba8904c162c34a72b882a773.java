package com.ipr.hometask.application;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ipr.hometask.files.FileUtils;
import com.ipr.hometask.model.ArgsModel;
import com.ipr.hometask.out.ListPrinter;
import com.ipr.hometask.utils.MapFilter;
import com.ipr.hometask.utils.TextParser;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Iwan on 01.06.2015
 */

public class StandaloneApplication {

    private static final Logger log = Logger.getLogger(StandaloneApplication.class);

    //    public static final String FILE_URL = "event_driven_architecture.txt";

    public static ArgsModel createArgsModel(String[] args) {
        ArgsModel argsModel = new ArgsModel();
        new JCommander(argsModel, args);
        return argsModel;
    }

    public static void main(String[] args) {
        try {
            ArgsModel argsModel = createArgsModel(args);
            System.out.println(argsModel);

            String fileUrl = argsModel.getFileList().get(0);

            int k = 10;

            List<String> stringList = FileUtils.fileToStringList(fileUrl);
            Map<String, Integer> dictionary = TextParser.parseStrings(stringList);
            Map<Integer, Set<String>> top = MapFilter.getTop(dictionary, k);

            ListPrinter.printFrequentWords(top, k);

        } catch (ParameterException e) {
            log.error(e.getMessage());
        }
    }
}