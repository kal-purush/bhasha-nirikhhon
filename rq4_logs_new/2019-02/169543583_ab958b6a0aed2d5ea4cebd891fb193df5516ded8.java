package filter;

import javax.servlet.*;
import javax.servlet.annotation.WebFilter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by igor on 2/6/19
 *
 * @author Yovkhomishch I.A.
 */
@WebFilter(filterName = "ipFilter", urlPatterns = {"/*"})
public class IpFilter implements Filter {

    private List<String> list;
    private String denied;
    private boolean b;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        b = false;
        list = new ArrayList<String>();
        Scanner in = null;

        try {
            in = new Scanner(new File("/home/igor/IdeaProjects/Black_List/src/main/resources/block"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        while (in.hasNextLine())
            list.add(in.nextLine());


        for (int i = 0; i < list.size(); i++) {
            denied = list.get(i);
            if (servletRequest.getRemoteAddr().equals(denied)) {
                b = true;

            }
        }

        if (b == true) {
            servletResponse.getWriter().write("Access disallowed.");
        } else {
            filterChain.doFilter(servletRequest, servletResponse);
            System.out.println(servletRequest.getRemoteAddr());
        }

    }


    @Override
    public void destroy() {
    }
}