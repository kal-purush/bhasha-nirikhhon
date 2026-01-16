package Exceptions.HW3.classes;

import Exceptions.HW3.abstracts.ACheckDataProcessor;
import Exceptions.HW3.abstracts.AFileWriter;
import Exceptions.HW3.abstracts.ALaunching;
import Exceptions.HW3.classes.parseData.CDataParseProcessor;
import Exceptions.HW3.classes.ui.CGetData;

import java.io.IOException;

public class CLaunching extends ALaunching {
    private final CGetData getData; // данные из ui
    private final CDataParseProcessor dataParseProcessor; // наш парсер
    private final ACheckDataProcessor dataProcessor; // проверки на валидность
    private final AFileWriter fileWriter; // наш записыватель (странный найминг:)) )

    public CLaunching(CGetData getData, CDataParseProcessor dataParseProcessor, ACheckDataProcessor dataProcessor, AFileWriter fileWriter) {
        this.getData = getData;
        this.dataParseProcessor = dataParseProcessor;
        this.dataProcessor = dataProcessor;
        this.fileWriter = fileWriter;
    }

    @Override
    public void run() throws IOException {
        dataParseProcessor.parseData(getData.getData());

        dataProcessor.checkQuantity(dataParseProcessor.getDataArray()); //проверка длинны
        dataProcessor.checkFullName(dataParseProcessor.getFullName()); // проверка ФИО
        dataProcessor.checkBirthday(dataParseProcessor.getBirthday()); // проверка дня рождения
        dataProcessor.checkSex(dataParseProcessor.getSex()); // проверка пола

        System.out.println("\nВведенные данные корректны");

        fileWriter.writeToFile(dataParseProcessor.getInfoToWrite()); // записываем


    }
}