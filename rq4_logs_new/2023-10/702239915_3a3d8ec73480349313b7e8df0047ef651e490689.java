package controller;

import java.io.IOException;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import model.Movie;
import model.MovieNight;
import controller.MovieHelper;
import controller.MovieNightHelper;

@WebServlet("/createMovieNight")
public class CreateMovieNightServlet extends HttpServlet {
    
    private MovieHelper movieHelper = new MovieHelper();
    private MovieNightHelper movieNightHelper = new MovieNightHelper();
    
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        List<Movie> movies = movieHelper.getAllMovies();
        request.setAttribute("movies", movies);
        request.getRequestDispatcher("/createMovieNight.jsp").forward(request, response);
    }
    
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String[] selectedMovieIds = request.getParameterValues("selectedMovies");
        String movieNightName = request.getParameter("movieNightName");
        
        MovieNight movieNight = new MovieNight(movieNightName);
        
        for (String movieIdStr : selectedMovieIds) {
            int movieId = Integer.parseInt(movieIdStr);
            Movie movie = movieHelper.getMovieById(movieId);
            movie.setMovieNight(movieNight);
            movieNight.getMovies().add(movie);
        }
        
        movieNightHelper.insertMovieNight(movieNight);
        
        response.sendRedirect("index.jsp");
    }
}