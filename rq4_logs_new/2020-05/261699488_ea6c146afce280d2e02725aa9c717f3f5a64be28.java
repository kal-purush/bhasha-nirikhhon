package ch.heigvd.gen2019.serializers;

import ch.heigvd.gen2019.Order;
import ch.heigvd.gen2019.Orders;
import ch.heigvd.gen2019.ProductWriter;

public class OrdersJsonSerializer extends OrdersSerializer {

    public String toParse(Orders orders){
        StringBuffer sb = new StringBuffer("{\"orders\": [");

        for (int i = 0; i < orders.getOrdersCount(); i++) {
            Order order = orders.getOrder(i);
            sb.append("{");
            sb.append("\"id\": ");
            sb.append(order.getOrderId());
            sb.append(", ");
            sb.append("\"products\": [");
            
            ProductWriter productWriter;
            ProductJsonSerializer productJsonSerializer = new ProductJsonSerializer();
            for (int j = 0; j < order.getProductsCount(); j++) {
                productWriter = new ProductWriter(order.getProduct(j),productJsonSerializer);
                sb.append(productWriter.getContents());
            }

            if (order.getProductsCount() > 0) {
                sb.delete(sb.length() - 2, sb.length());
            }

            sb.append("]");
            sb.append("}, ");
        }

        if (orders.getOrdersCount() > 0) {
            sb.delete(sb.length() - 2, sb.length());
        }

        return sb.append("]}").toString();
    }
}