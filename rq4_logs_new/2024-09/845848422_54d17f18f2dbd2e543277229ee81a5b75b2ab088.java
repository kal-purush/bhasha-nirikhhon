package org.example.controller;

import org.example.model.Location;
import org.example.service.locationService.ILocationService;
import org.example.service.locationService.LocationService;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.SQLException;

@WebServlet(name = "LocationServlet", urlPatterns = "/LocationServlet")
public class LocationServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private ILocationService iLocationService;

    public void init() {
        iLocationService = new LocationService();
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String action = request.getParameter("action");
        if (action == null) {
            action = "";
        }

        try {
            switch (action) {
                case "create":
                    showNewForm(request, response);
                    break;
                case "edit":
                    showEditForm(request, response);
                    break;
                case "delete":
                    deleteLocation(request, response);
                    break;
                default:
                    listLocation(request, response);
                    break;
            }
        } catch (SQLException ex) {
            throw new ServletException(ex);
        }
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String action = request.getParameter("action");
        if (action == null) {
            action = "";
        }
        try {
            switch (action) {
                case "create":
                    insertLocation(request, response);
                    break;
                case "edit":
                    updateLocation(request, response);
                    break;
            }
        } catch (SQLException ex) {
            throw new ServletException(ex);
        }
    }

    public void listLocation(HttpServletRequest request, HttpServletResponse response) throws SQLException, IOException, ServletException {
        request.setAttribute("locations", iLocationService.getAllLocations());
        RequestDispatcher dispatcher = request.getRequestDispatcher("location/list-locations.jsp");
        dispatcher.forward(request, response);
    }

    public void insertLocation(HttpServletRequest request, HttpServletResponse response) throws SQLException, IOException, ServletException {
        String locationName = request.getParameter("locationName");
        String mapLink = request.getParameter("mapLink");
        String imgURL = request.getParameter("imgURL");
        System.out.println(locationName);
        System.out.println(mapLink);

        if (locationName == null || mapLink == null) {
            request.getSession().setAttribute("message", "Vui lòng điền đầy đủ thông tin");
            request.getSession().setAttribute("status", "error");
            response.sendRedirect("LocationServlet?action=create");
        } else {
            Location location = new Location(locationName, mapLink, imgURL);
            iLocationService.insertLocation(location);
            request.getSession().setAttribute("message", "Thêm mới thành công!");
            request.getSession().setAttribute("status", "success");
            response.sendRedirect("LocationServlet");
        }
    }

    private void showNewForm(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        RequestDispatcher dispatcher = request.getRequestDispatcher("location/form-location.jsp");
        dispatcher.forward(request, response);
    }

    private void showEditForm(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        int locationId = Integer.parseInt(request.getParameter("locationId"));
        Location location = iLocationService.selectLocation(locationId);
        System.out.println(location);
        request.setAttribute("location", location);
        request.setAttribute("locations", iLocationService.getAllLocations());
        RequestDispatcher dispatcher = request.getRequestDispatcher("location/form-location.jsp");
        dispatcher.forward(request, response);
    }

    private void deleteLocation(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, SQLException {
        int locationId = Integer.parseInt(request.getParameter("locationId"));
        iLocationService.deleteLocation(locationId);
        request.getSession().setAttribute("message", "Xóa bài viết thành công!");
        request.getSession().setAttribute("status", "success");
        response.sendRedirect("LocationServlet?action=list");
    }

    private void updateLocation(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, SQLException {
        Location location = null;
        int locationId = Integer.parseInt(request.getParameter("locationId"));
        Location locationUpdate = iLocationService.selectLocation(locationId);
        String mapLink = request.getParameter("mapLink");
        String imgURL = request.getParameter("imgURL");
        location = new Location(locationId, locationUpdate.getLocationName(), mapLink, imgURL);
        iLocationService.updateLocation(location);
        request.getSession().setAttribute("message", "Cập nhật thành công!");
        request.getSession().setAttribute("status", "success");
        response.sendRedirect("LocationServlet?action=list");
    }
}