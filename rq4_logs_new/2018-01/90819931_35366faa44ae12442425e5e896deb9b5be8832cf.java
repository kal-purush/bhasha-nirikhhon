package org.lxp.multiple.thread.task;

import java.util.List;
import java.util.concurrent.RecursiveTask;

public class SumTask extends RecursiveTask<Integer> {
    private static final long serialVersionUID = 1L;
    private List<Integer> list;

    public SumTask(List<Integer> list) {
        this.list = list;
    }

    /**
     * Ensure it is necessary to divide the job to parts and finish them separately
     * 
     * @return
     */
    @Override
    protected Integer compute() {
        int rtn, size = list.size();
        if (size < 10) {
            rtn = sum(list);
        } else {
            SumTask subTask1 = new SumTask(list.subList(0, size / 2));
            SumTask subTask2 = new SumTask(list.subList(size / 2 + 1, size));
            subTask1.fork();
            subTask2.fork();
            rtn = subTask1.join() + subTask2.join();
        }
        return rtn;
    }

    private int sum(List<Integer> list) {
        return list.stream().mapToInt(number -> number.intValue()).sum();
    }
}