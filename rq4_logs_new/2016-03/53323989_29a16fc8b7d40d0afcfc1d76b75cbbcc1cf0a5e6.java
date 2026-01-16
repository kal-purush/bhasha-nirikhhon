/**
 *
 * Created by user on 07.03.2016.
 */
public class Cook implements Runnable {
    private int maksKebabCanProduct;
    private final Table table;
    private int indexOfKebabsMadeToDay;
    private String name;

    public Cook(int Y, Table table, String name) {
        this.maksKebabCanProduct = Y;
        this.table = table;
        this.indexOfKebabsMadeToDay = 0;
        this.name = name;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void run() {
        synchronized (table) {
            while (true) {
                service();
            }
        }

    }

    public Kebab[] makeKebab() {
        Kebab[] kebabProduced = new Kebab[maksKebabCanProduct];
        for (int i = 0; i < maksKebabCanProduct; i++) {
            kebabProduced[i] = new Kebab(indexOfKebabsMadeToDay++);
        }
        return kebabProduced;
    }

    private void service(){
        if (table.canPutMeals(maksKebabCanProduct)) {
            System.out.println("Cook " + name + " : Makes " + maksKebabCanProduct + " Kebab");
            try {
                Thread.sleep(1000);
                table.putMealsOnTable(makeKebab());
                table.notifyAll();
                table.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            try {
                System.out.println("Cook " + name + " : There's no space for kebab, Waiting");
                table.notify();
                table.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}