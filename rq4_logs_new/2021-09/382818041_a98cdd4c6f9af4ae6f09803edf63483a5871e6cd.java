import java.util.*;

public class atm {
    // массив Номиналов и количеств банкнот, отсортированный в обратном порядке(от большего к меньшему)
    private NavigableMap<Integer,Integer> holder = new TreeMap<>(Comparator.comparingInt(o ->o*-1));
    // массив доступных номиналов
    private final Integer[] availableDenominations = new Integer[]{1000,500,100,50,10};
    //
    public atm() {
    }
    // функция добавления купюр определённого номинала
    public void addMoney(Integer denomination, Integer count) {
       // проверяем - есть ли купюра с таким номиналом в списке досупных номиналов
       var isExists = Arrays.stream(availableDenominations).anyMatch(o -> o == denomination.intValue());
       // если есть - добавляем
       if (isExists) {
           // проверим есть ли хоть одна из таких купюр в банкомате
           var rest = holder.get(denomination);
           // если есть - обновляем количество купюр
           if (rest != null) {
               rest += count;
           // если нет - добавляем купюру
           } else {
               holder.put(denomination,count);
           }
       // если купюры с таким номиналом нет - возвращаем ошибку
       } else {
           System.out.println("Banknotes with denomination "+denomination+" do not exist!");
       }
    }

    // фунция показывает общий остаток денежных средств
    public Integer getRestMoney() {
        //return holder.values().stream().map(o->o.);
        return holder.entrySet()
                .stream()
                .map(o->o.getKey()*o.getValue())
                .reduce(0, Integer::sum);
    }

    // фунция показывает минимальный номинал в наличии
    public Integer getMinDenomination() {
        Integer res = 0;
        if (!holder.isEmpty()) {
            res = holder.lastKey();
        }
        return res;
    }

    // фунция выдаёт запрошенную сумму
    public void getMoney(Integer requestSum) {
        Integer totalMoney = getRestMoney();
        // проверка на то, что нам вообще хватит денег
        if (requestSum > totalMoney||totalMoney==0) {
            System.out.println("The requested sum "+requestSum + " exceeds the limit " + totalMoney);
            return;
        } else if (requestSum <= 0 || requestSum % this.getMinDenomination() != 0)
        {
            System.out.println("Minimum amount to be issued " + this.getMinDenomination());
            return;
        }
        // массив денег к выдаче
        NavigableMap<Integer,Integer> moneyForTake = new TreeMap<>(Comparator.comparingInt(o ->o*-1));
        Integer denomination;
        Integer quantity;
        Integer withdrawnMoneySum;
        Integer withdrawnMoneyCount;
        // пробежимся по мапе(она уже отсортирована от большего к меньшему)
        for (Map.Entry<Integer, Integer> Cell : holder.entrySet()) {
            denomination =  Cell.getKey();
            quantity = Cell.getValue();
            // количество купюр данного номинала к снятию
            withdrawnMoneyCount = Math.min(((int) Math.floor(requestSum / denomination)), quantity);
            // Сумма купюр данного номинала к снятию
            withdrawnMoneySum = withdrawnMoneyCount * denomination;
            // заполним массив денег к снятию
            moneyForTake.put(denomination, withdrawnMoneyCount);
            // уменьшим количество, которое осталось снять
            requestSum -= withdrawnMoneySum;
        }
        // если не смогли снять запрошенную сумму полностью - выведем ошибку
        if (requestSum != 0) {
            System.out.println("There is no way to withdraw the requested amount. Please increase the withdrawal rate");
            return;
        }
        // если до этого шага дошли - значит запрошенная сумма может быть корректно снята - снимем деньги
        cashWithdrawal(moneyForTake);
    }
    // фунция выдачи суммы банкоматом
    private void cashWithdrawal(NavigableMap<Integer,Integer> moneyForTake) {
        Integer denomination;
        Integer quantity;
        Integer quantityHolder;
        // пробежимся по снятым деньгам
        for (Map.Entry<Integer, Integer> Cell : moneyForTake.entrySet()) {
            denomination = Cell.getKey();
            quantity = Cell.getValue();
            quantityHolder = holder.get(denomination);
            // если количество списалось полностью - удалим запись
            if (quantityHolder <= quantity) {
                holder.remove(denomination);
            } else {
                quantityHolder = quantityHolder-quantity;
                holder.put(denomination,quantityHolder);
            }
            // "Выдача банкнот"
            System.out.println("Issuance of " + quantity + " bills of " + denomination + " denomination");
        }
    }
}