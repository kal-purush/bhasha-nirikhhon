package ru.otus.processor;

import ru.otus.model.Message;

import java.util.Calendar;
import java.util.Date;
import java.util.Optional;

public class ProcessorEvenSecondException implements Processor{

    private Date date;

    public ProcessorEvenSecondException(Date date) {
        this.date = date;
    }

    @Override
    public Message process(Message message) {

        if (this.date == null) {
            this.date = Calendar.getInstance().getTime().;
        }

//        Calendar.getInstance().get

        int currentSecond = Calendar.getInstance().get(Calendar.SECOND);
        if (currentSecond % 2 == 0) {
            try {
                throw new Exception("even second exception");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        this.date = null;

        return message;
    }

    public void setSecond(int second) {
        this.second = second;
    }
}