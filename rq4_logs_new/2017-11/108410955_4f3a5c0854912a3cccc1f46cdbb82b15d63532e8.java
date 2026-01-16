/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.harionovsky.bunstore.controllers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;
import ru.harionovsky.bunstore.models.Orders;
import ru.harionovsky.bunstore.models.Ware;
import ru.harionovsky.bunstore.models.Warehouse;
import ru.harionovsky.bunstore.utils.Basket;

/**
 *
 * @author Harionovsky
 */
@Controller
@RequestMapping("/order")
public class OrderController extends BaseController {
    
    @RequestMapping(method = RequestMethod.GET)
    public ModelAndView order(HttpServletRequest objRequest, HttpServletResponse objResponse) {
        Basket objBasket = new Basket(objRequest, objResponse);
        String[] arrBasket = objBasket.all();
        for (String itemB : arrBasket) {
            String[] arrLine = itemB.split("=");
            int iWareID = Integer.parseInt(arrLine[0]);
            int iCount = Integer.parseInt(arrLine[1]);
            Ware elemWare = dbBS.Ware.find(iWareID);
            if (elemWare != null) {
                Warehouse elemWH = dbBS.Warehouse.first("WareID = " + iWareID);
                if ((elemWH == null) || (elemWH.getQuantity() < iCount)) {
                    ModelAndView mvImpossible = new ModelAndView("impossible");
                    mvImpossible.addObject("message", "Извините, на складе нет товара \"" + elemWare.getName() + "\" в достаточном количестве");
                    return mvImpossible;
                }
            }
            else
                objBasket.del(iWareID);
        }
        
        if (objBasket.size() == 0) {
            ModelAndView mvImpossible = new ModelAndView("impossible");
            mvImpossible.addObject("message", "Корзина пуста");
            return mvImpossible;
        }
        
        ModelAndView mvOrder = new ModelAndView("order");
        
        return mvOrder;
    }
    
    
    @RequestMapping(method = RequestMethod.POST)
    public ModelAndView orderInsert(String FIO, String Phone, String Address, HttpServletRequest objRequest, HttpServletResponse objResponse) {
        //Basket objBasket = new Basket(objRequest, objResponse);
        // TODO: проверяем количество
        Orders elemOrder = new Orders(FIO, Phone, Address);
        dbBS.Order.insert(elemOrder);
        // TODO: заполняем таблицу "OrderWare"
        // TODO: списываем количество из таблицы "Warehouse"
        // TODO: добаляем кол-во в таблицу "Reserve"
        // TODO: очищаем "куки"
        return new ModelAndView("redirect:/home");
    }
}