package com.stl.tags;

import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.support.RequestContextUtils;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.PageContext;
import javax.servlet.jsp.tagext.SimpleTagSupport;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

public class STLTag extends SimpleTagSupport {
    protected WebApplicationContext getWebApplicationContext() {
        PageContext pageContext = (PageContext) getJspContext();
        return RequestContextUtils.getWebApplicationContext(pageContext.getRequest());
    }

    protected Object getFromApplicationContext(String name) {
        return getWebApplicationContext().getBean(name);
    }

    protected Object getFromApplicationContext(Class name) {
        return getWebApplicationContext().getBean(name);
    }

    private String processJspBody() throws IOException, JspException {
        Writer writer = new StringWriter();
        getJspBody().invoke(writer);

        return writer.toString();
    }

    protected void renderJspBody() throws JspException, IOException {
        PageContext pageContext = (PageContext) getJspContext();
        pageContext.getOut().write(processJspBody());
    }
}