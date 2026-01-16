package net.ramon.enesimatabla;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import com.google.gson.Gson;
import java.util.Random;

/**
 * Servlet implementation class Controller
 */
public class Control extends HttpServlet {

    private static final long serialVersionUID = 1L;

    /**
     * Default constructor.
     */
    public Control() {
        // TODO Auto-generated constructor stub
    }

    /**
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
     * response)
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        // TODO Auto-generated method stub
        // response.getWriter().append("Served at: ").append(request.getContextPath());
        processRequest(request, response);
    }

    /**
     * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
     * response)
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        // TODO Auto-generated method stub
        doGet(request, response);
    }

    protected boolean processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("application/json");
        PrintWriter out = response.getWriter();
        Gson gSon = new Gson();
        String stalto = request.getParameter("num2").trim();
        String stancho = request.getParameter("num1").trim();
        String er = "^[1-9]\\d*$";

        try {

            Random r = new Random();
            int Low = 500;
            int High = 3000;
            int Result = r.nextInt(High - Low) + Low;
            Thread.sleep(Result);

            if (stalto.matches(er) && stancho.matches(er)) {
                Integer num2 = Integer.parseInt(request.getParameter("num2"));
                Integer num1 = Integer.parseInt(request.getParameter("num1"));
                String operacion = request.getParameter("desp");
                Integer[] resultado = new Integer[1];

                switch (operacion) {
                    case "mul":
                        resultado[0] = num1 * num2;
                        break;
                    case "add":
                        resultado[0] = num1 + num2;
                        break;
                    case "minus":
                        resultado[0] = num1 - num2;
                        break;
                    case "div":
                        resultado[0] = num1 / num2;
                        break;
                    default:
                        break;
                }
                response.getWriter().append(gSon.toJson(resultado));

            } else {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException iae) {
            response.setStatus(500);
            String error;
            if (stalto.equals("") || stancho.equals("")) {
                error = "Rellene los campos vacíos.";
            } else {
                error = "Solo puede introducir números enteros o decimales.";
            }
            String strJson = gSon.toJson(error);
            out.print(strJson);
        } catch (InterruptedException ex){
            ex.printStackTrace();
        }
        return false;
    }
}