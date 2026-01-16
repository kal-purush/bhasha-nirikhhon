package com.artpro.service;

import com.artpro.model.GroundTransport;
import com.artpro.model.Transport;

//    Для легкового разработать метод который будет принимать время и считать
//    сколько километров проедет машина двигаясь с максимальной скоростью и
//    сколько топлива она израсходует за это время, результат вывести в консоль,
//    расчёт израсходуемого топлива вынести в отдельный метод private.
public class PassengerTransportService {
    //конечная цель метода - вывод, по этому используем void
    public void calculationPassengerTransport(Transport transport, double time) {
        //в параметрах метода calculationPassengerTransport мы будем принимать поля
        //из класса Transport, заведём ссылку transport через которую будем обращаться
        // и принимать значение времени по условию задачи
        double way = transport.getMaxSpeed() * time; //расчёт дистанции
        //формула потраченного топлива = расход топлива умножаем на нашу дистанцию и делим на 100
        //поле расхода топлива в классе Transport нету, а оно есть в GroundTransport
        //обращаемся к этому полю
        GroundTransport groundTransport = (GroundTransport) transport;
        //теперь через groundTransport подключаем поле fuelСonsumption
        double fuel = groundTransport.getFuelСonsumption() * way / 100; //пишем математику
        //теперь выедем всё в консоль
//        Результат работы метода должен вывести такую строчку:
//        За время (ваше введённое время) ч, автомобиль (марка автомобиля)
//        двигаясь с максимальной скоростью (ваша максимальная скорость) км/ч
//        проедет (xxx) км и израсходует (xxx) литров топлива.
        System.out.println("За время " + time + " ч, автомобиль " + transport.getBrand() +
               " двигаясь с максимальной скоростью " + transport.getMaxSpeed() + "км/ч проедет " +
                way + " км и израсходует " + fuel + " литров топлива");
    }
}