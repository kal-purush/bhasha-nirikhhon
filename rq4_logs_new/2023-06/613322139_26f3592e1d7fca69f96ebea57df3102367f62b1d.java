import java.util.*;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CaseInATM implements Case {

    private Map<Nominal,CellInCase> allCells = new TreeMap<>();

    CaseInATM(){
        for(Nominal nominal:Nominal.values()){
            allCells.put(nominal,new CellInCase());
        }
    }

    public Map<Integer, Integer> getMoney(int money) {
        Map<Integer,Integer> gettingMoney = new TreeMap<>();

        Map<Nominal,CellInCase> copyAllCells = allCells.entrySet()
                .stream()
                .map(it-> Map.entry(it.getKey(),new CellInCase(it.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue,(oldValue,newValue)->newValue,TreeMap::new));

        for(Map.Entry<Nominal,CellInCase> m: copyAllCells.entrySet()){
            int count = 0;
            while(money>=m.getKey().getNominalValue()&&(m.getValue()).countCell.size() > 0) {
                if (money - m.getKey().getNominalValue() >= 0) {
                    money -= m.getKey().getNominalValue();
                    (m.getValue()).getNominal(Nominal.getNominal(m.getKey().getNominalValue()));
                    count = count + 1;
                    gettingMoney.put(m.getKey().getNominalValue(),count);
                }
            }
        }
        if(money>0) {
            throw new TakeMoneyOutException("В банкомате недостаточно купюр для выдачи запрошенной суммы");
        }
        allCells = copyAllCells;
        return gettingMoney;
    }

    public void put(Nominal nominal){
        allCells.get(nominal).addNominal(nominal);
    }

    public int getBalance() {
        int balance = 0;
        for(Map.Entry<Nominal,CellInCase> m: allCells.entrySet()){
            balance += m.getKey().getNominalValue() * (m.getValue()).countCell.size();
        }
        return balance;
    }
}