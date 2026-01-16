package com.mate.controller.car;

import com.mate.lib.Injector;
import com.mate.model.Car;
import com.mate.service.CarService;
import com.mate.service.ManufacturerService;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class AddCarController extends HttpServlet {
    private static final Injector injector = Injector.getInstance("com.mate");
    private final CarService carService = (CarService) injector
            .getInstance(CarService.class);
    private final ManufacturerService manufacturerService = (ManufacturerService) injector
            .getInstance(ManufacturerService.class);

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        req.getRequestDispatcher("/WEB-INF/views/cars/add.jsp").forward(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws IOException, ServletException {
        carService.create(new Car(
                req.getParameter("model"),
                manufacturerService.get(
                        Long.parseLong(req.getParameter("manufacturer_id")))));
        req.getRequestDispatcher("/WEB-INF/views/cars/add.jsp").forward(req, resp);
    }
}