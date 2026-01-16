package Transport;

import java.util.Queue;

public class ServiceStation {
    private Queue<Bus> queue;
    private Queue<Transport> queue1;

    public  ServiceStation(Queue<Bus> queue) {
        toString();
        System.out.println("Не проходит тех обслуживание");

    }
    public  ServiceStation(Queue<Transport> queue1){

    }


    /*while(poll(ServiceStation)!= null) {
        System.out.println(getClass());
    }*/

    /*public void addTransport(Transport transport){
        if (transport != null) {
            System.out.println("Введите автомобиль");
        }
        queue.offer(transport);
    }
    public  void  carryOutATechnicalInspection(Transport transport){
        for(Transport transports: queue){
            System.out.println("проходим ТО");
            queue.remove();
        }
    }*/
    /*Е element() - возвращает элемент из головы очереди. Элемент не удаляется. Если очередь пуста, инициируется исключение NoSuchElementException.
Е remove() - удаляет элемент из головы очереди, возвращая его. Инициирует исключение NoSuchElementException, если очередь пуста.
Е peek() - возвращает элемент из головы очереди. Возвращает null, если очередь пуста. Элемент не удаляется.
Е роll() - возвращает элемент из головы очереди и удаляет его. Возвращает null, если очередь пуста.
boolean offer(Е оbj) - пытается добавить оbj в очередь. Возвращает true, если оbj добавлен, и false в противном случае.*/

}