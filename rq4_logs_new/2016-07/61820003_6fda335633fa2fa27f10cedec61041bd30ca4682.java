package com.action;

import com.hackerspace.dao.Studio;
import com.opensymphony.xwork2.ActionSupport;
import org.apache.struts2.ServletActionContext;
import org.apache.struts2.convention.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by Administrator on 2014/12/19.
 */

@ParentPackage("struts-default")
@Namespace("/")
@Results({
        @Result(name="success", location="/index.jsp")
})
public class SayHelloAction extends ActionSupport{

    @Action(value = "hi")
    public String sayHello() throws IOException {

        System.out.println("hi");


        Logger logger = LoggerFactory.getLogger(Studio.class);

        logger.info("INFO");
        logger.debug("DEBUG");
        logger.error("ERROR");


        while(true) {
            ;
        }




    }

    private String name;

    public String getName() {
        return name;
    }


    public void setName(String name) {
        this.name = name;

        System.out.println(name + "name ");
    }


}