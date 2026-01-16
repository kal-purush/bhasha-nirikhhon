package com.greedy.section01.xmlconfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.greedy.section01.xmlconfig.controller.MenuController;
import com.greedy.section01.xmlconfig.service.MenuService;

public class Application01 {

   public static void main(String[] args) {
      Scanner sc = new Scanner(System.in);
      
      ApplicationContext context =
    		  new AnnotationConfigApplicationContext("com.greedy.section01");
      
      MenuController menuController = context.getBean(MenuController.class);
      
      do {
         System.out.println("====== �޴� ���� =====");
         System.out.println("1. �޴� ��ü ��ȸ");
         System.out.println("2. �޴� �ڵ�� ��ȸ");
         System.out.println("3. �ű� �޴� �߰�");
         System.out.println("4. �޴� ����");
         System.out.println("5. �޴� ����");
         System.out.println("�޴� ���� ��ȣ�� �Է����ּ��� : ");
         int no = sc.nextInt();
         
         switch (no) {
         case 1: menuController.findAllMenus();
               break;
         case 2: menuController.findMenuByMenuCode(inputMenuCode());
         break;
         
         case 3: menuController.registNewMenu(inputMenu());
         break;
         
         case 4: menuController.modifyMenu(inputModifyMenu());
         break;
         
         case 5: menuController.removeMenu(inputMenuCode());
         break;
         
         default:
            System.out.println("�߸��� �޴��� �����Ͽ����ϴ�.");
               break;
         }
         
      }while(true);
   }
   
   private static Map<String, String> inputMenuCode(){
      Scanner sc = new Scanner(System.in);
      System.out.println("�޴� �ڵ� �Է� : ");
      String code = sc.nextLine();
      
      Map<String, String> parameter = new HashMap<String, String>();
      parameter.put("code", code);
      
      return parameter;
   }
   private static Map<String, String> inputMenu(){
      Scanner sc = new Scanner(System.in);
      System.out.println("�޴� �̸� �Է� : ");
      String name = sc.nextLine();
      
      
      System.out.println("�޴��� ������ �Է� : ");
      String price = sc.nextLine();
      
      System.out.println("ī�װ��� �ڵ带 �Է� : ");
      String categoryCode = sc.nextLine();
      

      Map<String, String> parameter = new HashMap<>();
      parameter.put("name", name);
      parameter.put("price", price);
      parameter.put("categoryCode", categoryCode);
      
      return parameter;

   }
   
   private static Map<String, String> inputModifyMenu(){
      Scanner sc = new Scanner(System.in);
      System.out.println("���� �� �޴� �ڵ� �Է� : ");
      String code = sc.nextLine();
      
      System.out.println("���� �� �޴� �̸� �Է� : ");
      String name = sc.nextLine();
      
      System.out.println("���� �� �޴��� ������ �Է� : ");
      String price = sc.nextLine();
      
      System.out.println("���� �� ī�װ��� �ڵ带 �Է� : ");
      String categoryCode = sc.nextLine();
      

      Map<String, String> parameter = new HashMap<>();
      parameter.put("code", code);
      parameter.put("name", name);
      parameter.put("price", price);
      parameter.put("categoryCode", categoryCode);
      
      return parameter;
   }
   
}