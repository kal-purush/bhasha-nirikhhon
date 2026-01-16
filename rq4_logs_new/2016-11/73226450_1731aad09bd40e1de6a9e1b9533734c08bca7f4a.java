/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interceptors;

import com.opensymphony.xwork2.ActionContext;
import com.opensymphony.xwork2.ActionInvocation;
import com.opensymphony.xwork2.interceptor.Interceptor;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sergio
 */
public class SaveParamsOnSession implements Interceptor {

    @Override
    public void destroy() {
        Logger.getLogger(SaveParamsOnSession.class.getName()).log(Level.INFO, "Destroy SaveParamsOnSession Interceptor ...");
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void init() {
        Logger.getLogger(SaveParamsOnSession.class.getName()).log(Level.INFO, "Init SaveParamsOnSession Interceptor ...");
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String intercept(ActionInvocation invocation) throws Exception {
        Logger.getLogger(SaveParamsOnSession.class.getName()).log(Level.INFO, "Executing SaveParamsOnSession Interceptor ...");
        final ActionContext context = invocation.getInvocationContext();
        Map params = context.getParameters();
        Set<Map.Entry<String, Object>> entries = params.entrySet();
         Logger.getLogger(SaveParamsOnSession.class.getName()).log(Level.INFO, "Params size: " + entries.size());
        for ( Map.Entry<String, Object> entry : entries ) {
            Logger.getLogger(SaveParamsOnSession.class.getName()).log(Level.INFO, entry.getKey());
        }
        return invocation.invoke();
    }
}