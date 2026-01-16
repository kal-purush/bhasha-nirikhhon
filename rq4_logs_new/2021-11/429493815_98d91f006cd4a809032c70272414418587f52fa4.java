package com.esai.vendingmachine;

import com.esai.vendingmachine.controller.VendingMachineController;
import com.esai.vendingmachine.dao.ClassVendingMachineAuditDao;
import com.esai.vendingmachine.dao.ClassVendingMachineAuditDaoFileImpl;
import com.esai.vendingmachine.dao.ClassVendingMachineDaoException;
import com.esai.vendingmachine.dao.ClassVendingMachinePersistenceException;
import com.esai.vendingmachine.dao.VendingMachineDao;
import com.esai.vendingmachine.dao.VendingMachineDaoFileImpl;
import com.esai.vendingmachine.service.InsufficientFundsException;
import com.esai.vendingmachine.service.NoItemInventoryException;
import com.esai.vendingmachine.service.VendingMachineServiceLayer;
import com.esai.vendingmachine.service.VendingMachineServiceLayerImpl;
import com.esai.vendingmachine.ui.UserIO;
import com.esai.vendingmachine.ui.UserIOConsoleImpl;
import com.esai.vendingmachine.ui.VendingMachineView;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 *
 * @author Esai
 */
public class App {

    public static void main(String[] args) throws InsufficientFundsException, NoItemInventoryException, ClassVendingMachineDaoException, ClassVendingMachinePersistenceException {
        /*UserIO myIO = new UserIOConsoleImpl();
        VendingMachineView myView = new VendingMachineView(myIO);
        VendingMachineDao myDao = new VendingMachineDaoFileImpl();
        ClassVendingMachineAuditDao myAuditDao = new ClassVendingMachineAuditDaoFileImpl();
        VendingMachineServiceLayer myService = new VendingMachineServiceLayerImpl(myDao, myAuditDao);
        VendingMachineController controller = new VendingMachineController(myView, myService);
        controller.run();*/

        ApplicationContext appContext = new ClassPathXmlApplicationContext("classpath:applicationContext.xml");

        VendingMachineController controller = appContext.getBean("controller", VendingMachineController.class);
        controller.run();
    }
}