
import java.util.ArrayList;
import java.util.Scanner;
public class Main {

    public static void main(String[] args) {
        Scanner scn = new Scanner(System.in);

        ArrayList<person> pp = new ArrayList<>();

        boolean flag = true;
        while (flag) {
            System.out.println("使用者好" + "\n" + "1--新增角色" + "\n" + "2--修改角色" + "\n" + "3--查詢角色" + "\n" + "4--結束");
            int a = scn.nextInt();
            switch (a) {
                case 1:
                    System.out.print("角色名稱:");
                    String name = scn.next();
                    System.out.print("性別:");
                    String sex = scn.next();
                    System.out.print("攻擊力:");
                    String atk = scn.next();
                    System.out.print("屬性:");
                    String property = scn.next();
                    person p = new person(name, sex, atk, property);
                    pp.add(p);
                    break;
                case 2:
                    System.out.println("請問要修改甚麼呢");
                    System.out.println("修改角色名稱--1");
                    System.out.println("修改性別--2");
                    System.out.println("修改攻擊力--3");
                    System.out.println("修改屬性--4");
                    String ss = scn.next();
                    System.out.println("請輸入名稱");

                    String name3 =scn.next();
                    for (int i =0;i<pp.size();i++){
                      if (pp.get(i).getName().equals(name3)){
                          System.out.println("沒有改變");
                      }
                       else {   pp.get(i).setName(name3);
                          System.out.println("修改完畢");
                      }
                    }

                    break;
                case 3:
                    System.out.println("用角色名稱查詢--1");
                    System.out.println("用性別查詢--2");
                    System.out.println("用攻擊力查詢--3");
                    System.out.println("用屬性查詢--4");

                        int choice = scn.nextInt();
                    switch (choice) {
                        case 1:
                            System.out.println("請輸入名稱");
                            String name2 = scn.next();
                            for (int i = 0; i < pp.size(); i++) {
                                if (pp.get(i).getName().equals(name2)) {
                                    System.out.println("角色名稱:" + pp.get(i).getName());
                                    System.out.println("性別:" + pp.get(i).getSex());
                                    System.out.println("攻擊力:" + pp.get(i).getAtk());
                                    System.out.println("屬性:" + pp.get(i).getProperty());
                                }
                            }
                            break;

                        case 2:
                            System.out.println("請輸入性別");
                            String sex2 = scn.next();
                            for (int i = 0; i < pp.size(); i++) {
                                if (pp.get(i).getSex().equals(sex2)) {
                                    System.out.println("角色名稱:" + pp.get(i).getName());
                                    System.out.println("性別:" + pp.get(i).getSex());
                                    System.out.println("攻擊力:" + pp.get(i).getAtk());
                                    System.out.println("屬性:" + pp.get(i).getProperty());
                                }
                            }
                            break;

                        case 3:
                            System.out.println("請輸入攻擊力");
                            String atk2 = scn.next();
                            for (int i = 0; i < pp.size(); i++) {
                                if (pp.get(i).getAtk().equals(atk2)) {
                                    System.out.println("角色名稱:" + pp.get(i).getName());
                                    System.out.println("性別:" + pp.get(i).getSex());
                                    System.out.println("攻擊力:" + pp.get(i).getAtk());
                                    System.out.println("屬性:" + pp.get(i).getProperty());
                                }
                            }
                            break;

                        case 4:
                            System.out.println("請輸入屬性");
                            String property2 = scn.next();
                            for (int i = 0; i < pp.size(); i++) {
                                if (pp.get(i).getProperty().equals(property2)) {
                                    System.out.println("角色名稱:" + pp.get(i).getName());
                                    System.out.println("性別:" + pp.get(i).getSex());
                                    System.out.println("攻擊力:" + pp.get(i).getAtk());
                                    System.out.println("屬性:" + pp.get(i).getProperty());
                                }
                            }
                            break;


                    }
                    break;

                case 4:
                    flag = false;
                    System.out.println("感謝使用");
                    break;
    }
}}}