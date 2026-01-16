package cz.cvut.iss.sysint;

import cz.cvut.iss.sysint.exception.CancelOrderException;
import cz.cvut.iss.sysint.model.Order;
import cz.cvut.iss.sysint.service.OrderRepository;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.model.RouteDefinition;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Filip
 */
public class AccountingRouteTest extends OrderProcessRouteTest {

    @EndpointInject(uri = "direct:accounting")
    private ProducerTemplate producer;

    @EndpointInject(uri = "mock:accounting")
    private MockEndpoint accounting;

    @EndpointInject(uri = "mock:exception")
    private MockEndpoint exception;

    @Before
    public void interceptHttp4() throws Exception {

    }

    @Test
    public void validInvoiceTest() throws Exception {

        RouteDefinition route = context.getRouteDefinitions().get(0);
        route.adviceWith(context, new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                interceptSendToEndpoint("ref:accountingHttp")
                        .skipSendToOriginalEndpoint()
                        .to(accounting);
                onException(CancelOrderException.class)
                        .log("CancelOrderException")
                        .handled(true)
                        .to("mock:exception");

            }
        });

        accounting.expectedMessageCount(1);
        producer.sendBody(OrderRepository.get(1L));

        accounting.assertIsSatisfied();
        assertTrue(accounting.getExchanges().get(0).getIn().getBody(String.class).startsWith("{\"id\":1,\"address\""));

    }

    @Test
    public void invalidInvoiceTest() throws Exception {

        RouteDefinition route = context.getRouteDefinitions().get(0);
        route.adviceWith(context, new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                interceptSendToEndpoint("ref:accountingHttp")
                        .skipSendToOriginalEndpoint()
                        .process(exchange -> {
                            exchange.getOut().setBody("{\"invoiceId\":-1");
                        });
                onException(CancelOrderException.class)
                        .log("CancelOrderException")
                        .handled(true)
                        .to("mock:exception");

            }
        });

        exception.expectedMessageCount(1);

        Order order = OrderRepository.get(1L);
        order.setId(-1L);

        producer.sendBody(order);

        exception.assertIsSatisfied();

    }

    @Override
    protected String getTestedRouteName() {
        return "accounting";
    }
}