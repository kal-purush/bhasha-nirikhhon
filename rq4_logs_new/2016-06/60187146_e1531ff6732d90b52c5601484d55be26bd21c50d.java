package server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class ServiceDispatcher {
    private static ApplicationContext springContext;

    @Autowired
    public void init(ApplicationContext springContext) {
        ServiceDispatcher.springContext = springContext;
    }

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    public static HandlersApi dispatch(Map<String, String> requestMap) {
        String serviceUri = requestMap.get("REQUEST_URI");
        String beanName = null;

        if (serviceUri == null) {
            beanName = "notFound";
        }

        if (serviceUri.startsWith("/tokens")) {
            String httpMethod = requestMap.get("REQUEST_METHOD");

            switch (httpMethod) {
                case "POST":
                    beanName = "tokenIssue";
                    break;
                case "DELETE":
                    beanName = "tokenExpier";
                    break;
                case "GET":
                    beanName = "tokenVerify";
                    break;

                default:
                    beanName = "notFound";
                    break;
            }
        }
        else if (serviceUri.startsWith("/users")) {
            beanName = "users";
        }
        else {
            beanName = "notFound";
        }

        HandlersApi service;
        try{
            service = (HandlersApi) springContext.getBean(beanName, requestMap);
        }
        catch (Exception e) {
            e.printStackTrace();
            service = (HandlersApi) springContext.getBean("notFound", requestMap);
        }

        return service;
    }
}