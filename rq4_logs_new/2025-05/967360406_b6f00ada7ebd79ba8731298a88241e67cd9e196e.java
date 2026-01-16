package main.managers.history;

import main.model.Task;

import java.util.ArrayList;
import java.util.List;

public class InMemoryHistoryManager implements HistoryManager { // Реализация истории просмотров в памяти

    private static List<Task> history = new ArrayList<>();

    /* Вопрос.  Здравствуйте, Александр!
    Вы сказали, что нужно добавить константу.
    - Почему именно константа(речь я так понимаю про модификатор static)?
    - Должали она быть приватной или публичной?
    - Стоит ли делать её статической(модификатор final)?
    Интернет мне подсказал сделать такую запись (см.ниже), но я не понимаю, что это уместно именно здесь.
     */
    public static final int HISTORY_MAX_SIZE = 10;

    @Override
    public void add(Task task) {
        if (history.size() >= HISTORY_MAX_SIZE) {
            history.remove(0);
        }
        history.add(task);
        /* Коментарий ревьюера.
        по условию должен быть тест менеджерра истории, что после добавления таска в историю и изменения его
        в истории должна остаться предыдущая версия, сейчас это работать не будет

        Не понимаю, Вас. Я проверял через main так:
        1) Создал задачу, вызвал её. Она после вызова сохранилась в history.
        2) Потом произвёл обновление задачи (поменял имя, описание, статус).
        3) Вывел на печать эту самую задачу (это список который сохранён в InMemoryTaskManager - taskMap)
        и вывел список последних посмотренных задач (это список который сохранён в  InMemoryHistoryManager).
        И они разные списки. В taskMap обновлённая задача, в history старая. Они же не синхронизированы. И обновление
        history происходит только когда идёт просмотр.
         */
    }


    @Override
    public List<Task> getHistory() {
        return new ArrayList<>(history);
    }
}