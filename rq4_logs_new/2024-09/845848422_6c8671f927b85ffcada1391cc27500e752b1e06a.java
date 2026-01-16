package org.example.controller;

import org.example.model.Location;
import org.example.model.Post;
import org.example.service.locationService.LocationService;
import org.example.service.postService.PostService;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

@WebServlet("")
public class HomeServlet extends HttpServlet {
    private PostService postService = new PostService();
    private LocationService locationService = new LocationService();

    @Override
    public void init() {
        postService = new PostService();
        locationService = new LocationService();
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        List<Post> posts = postService.getAllPosts();
        List<Location> locations = locationService.getAllLocations();

        if (posts.size() > 3) {
            posts = posts.subList(0, 3);
        }
        request.setAttribute("posts", posts);
        request.setAttribute("locations", locations);
        RequestDispatcher dispatcher = request.getRequestDispatcher("index.jsp");
        dispatcher.forward(request, response);
    }
}
