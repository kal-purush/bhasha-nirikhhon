package study.proxy;

import study.proxy.annotation.Proxy;

/**
 * jiangyukun on 2015-08-24.
 */
@Proxy(proxyName = "study.proxy.CellPhoneProxy")
public class CellPhone {
    public void start() {
        System.out.println("starting cellphone...");
    }
}