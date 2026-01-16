package com.cognodyne.dw.common;

import java.util.Collections;
import java.util.List;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameParser;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JndiSupport {
    private static final Logger logger = LoggerFactory.getLogger(JndiSupport.class);

    public static void bind(String name, Object value) throws NamingException {
        Context ctx = null;
        try {
            ctx = new InitialContext();
            ctx.rebind(name, value);
            logger.debug("successfully bound {} to {}", value, name);
        } catch (NamingException e) {
            ensureSubContextsExist(ctx, name);
            ctx.rebind(name, value);
            logger.debug("successfully bound {} to {}", value, name);
        } finally {
            ctx.close();
        }
    }

    public static Object lookup(String name) {
        InitialContext ctx = null;
        try {
            ctx = new InitialContext();
            return ctx.lookup(name);
        } catch (NamingException ex) {
            logger.error("Unable to lookup {}", name, ex);
            return null;
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (NamingException e) {
                }
            }
        }
    }

    private static void ensureSubContextsExist(Context ctx, String name) throws NamingException {
        NameParser parser = ctx.getNameParser(name);
        List<String> parts = Collections.list(parser.parse(name).getAll());
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < parts.size() - 1; i++) {
            if (i > 0) {
                buf.append("/");
            }
            buf.append(parts.get(i));
            String currentContext = buf.toString();
            if (!exists(ctx, currentContext)) {
                ctx.createSubcontext(currentContext);
            }
        }
    }

    private static boolean exists(Context ctx, String name) {
        try {
            return ctx.lookup(name) != null;
        } catch (NamingException e) {
            return false;
        }
    }
}