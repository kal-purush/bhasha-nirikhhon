/*
 * Класс статистики. 
 * Сюда заносятся покупатели после совершения покупки, с указанием потраченных средств. 
 */

package lesson2;

import java.util.ArrayList;
import java.util.List;

public class Statistic {
    private List<Сustomer> logs;

    public Statistic() {
        logs = new ArrayList<>();
    }

    public int getSize() {
        return logs.size();
    }

    public List<Сustomer> getLogsRecord() {
        return logs;
    }

    public void setLogs(Сustomer cСustomer) {
        this.logs.add(cСustomer);
    }

}