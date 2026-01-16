package ksbysample.webapp.demo;

import ksbysample.library.dependnospring.Order;
import ksbysample.library.dependnospring.OrderBuilder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/order")
public class OrderController {

    @RequestMapping
    @ResponseBody
    public String index() {
        Order order = new OrderBuilder()
                .number(5)
                .price(2000)
                .build();
        return String.format("数量 = %d, 単価 = %d, 総額 = %d"
                , order.getNumber().intValue()
                , order.getPrice().intValue()
                , order.getTotal().intValue());
    }

}