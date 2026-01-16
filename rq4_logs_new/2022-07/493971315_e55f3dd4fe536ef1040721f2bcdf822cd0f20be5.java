package homework.oopHomework;

import java.util.Date;

public class Example {

    double galaSumma;
    public static void main(String[] args) {
        Customer customer1 = new Customer("Ilze");
        customer1.setMemberType("Premium");
        customer1.setMember(true);
        Visit visit1 = new Visit(customer1, new Date());
        visit1.setServiceExpense(20.10);
        visit1.setProductExpense(10.50);
        System.out.println(visit1.toString());
        DiscountRate discountRate = new DiscountRate();
        double galaSumma = ((visit1.getServiceExpense()-(visit1.getServiceExpense()*discountRate.getServiceDiscountPremium()))+
                (visit1.getProductExpense()-(visit1.getProductExpense()*discountRate.getProductDiscountPremium())));
        System.out.println("Klients 1, kopejie izdevumi " + visit1.getTotalExpenses() +
                ", servisa atlaide " + discountRate.getServiceDiscountPremium() +
                ", produktu atlaide " + discountRate.getProductDiscountPremium() +
                ", gala summa, kas jaapmaksa ir " + galaSumma);
        System.out.println();


        Customer customer2 = new Customer("Aija");
        customer2.setMemberType("Gold");
        customer2.setMember(true);
        Visit visit2 = new Visit(customer2, new Date());
        visit2.setServiceExpense(30.1);
        visit2.setProductExpense(20);
        System.out.println(visit2.toString());
        DiscountRate discountRate2 = new DiscountRate();
        double galaSumma2 = ((visit2.getServiceExpense()-(visit2.getServiceExpense()*discountRate.getServiceDiscountGold()))+
                (visit2.getProductExpense()-(visit2.getProductExpense()*discountRate.getProductDiscountGold())));
        System.out.println("Klients 2, kopejie izdevumi " + visit2.getTotalExpenses() +
                ", servisa atlaide " + discountRate2.getServiceDiscountGold() +
                ", produktu atlaide " + discountRate2.getProductDiscountGold() +
                ", gala summa, kas jaapmaksa ir " + galaSumma2);
        System.out.println();


        Customer customer3 = new Customer("Vija");
        customer3.setMemberType("Silver");
        customer3.setMember(true);
        Visit visit3 = new Visit(customer3, new Date());
        visit3.setServiceExpense(50);
        visit3.setProductExpense(20);
        System.out.println(visit3.toString());
        DiscountRate discountRate3 = new DiscountRate();
        double galaSumma3 = ((visit3.getServiceExpense()-(visit3.getServiceExpense()*discountRate.getServiceDiscountSilver()))+
                (visit3.getProductExpense()-(visit3.getProductExpense()*discountRate.getProductDiscountSilver())));
        System.out.println("Klients 3, kopejie izdevumi " + visit3.getTotalExpenses() +
                ", servisa atlaide " + discountRate3.getServiceDiscountSilver() +
                ", produktu atlaide " + discountRate3.getProductDiscountSilver() +
                ", gala summa, kas jaapmaksa ir " + galaSumma3);
        System.out.println();


        Customer customer4 = new Customer("Eva");
        customer4.setMemberType("Non customer");
        customer4.setMember(true);
        Visit visit4 = new Visit(customer4, new Date());
        visit4.setServiceExpense(30);
        visit4.setProductExpense(10);
        System.out.println(visit4.toString());
        DiscountRate discountRate4 = new DiscountRate();
        double galaSumma4 = visit4.getTotalExpenses();
        System.out.println("Klients 4, kopejie izdevumi " + visit4.getTotalExpenses() +
                ", servisa atlaide " + discountRate4.getServiceDiscountNonCustomer() +
                ", produktu atlaide " + discountRate4.getServiceDiscountNonCustomer() +
                ", gala summa, kas jaapmaksa ir " + galaSumma4);

    }
}