package org.example.controller;

import org.example.model.*;
import org.example.service.categoryService.CategoryService;
import org.example.service.commentService.CommentService;
import org.example.service.locationService.LocationService;
import org.example.service.postService.PostService;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

@WebServlet(name = "PostServlet", urlPatterns = "/PostServlet")
public class PostServlet extends HttpServlet {
    private PostService postService;
    private CommentService commentService;
    private LocationService locationService;
    private CategoryService categoryService;

    public void init() {
        postService = new PostService();
        commentService = new CommentService();
        locationService = new LocationService();
        categoryService = new CategoryService();
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String action = request.getParameter("action");
        if (action == null) {
            action = "list";
        }

        loadCategories(request);
        try {
            switch (action) {
                case "list":
                    listPosts(request, response);
                    break;
                case "new":
                    showNewForm(request, response);
                    break;
                case "delete":
                    deletePost(request, response);
                    break;
                case "edit":
                    showEditForm(request, response);
                    break;
                case "view":
                    viewPost(request, response);
                    break;
                case "deleteComment":
                    deleteComment(request, response);
                    break;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String action = request.getParameter("action");
        if (action == null) {
            action = "";
        }
        try {
            switch (action) {
                case "insert":
                    insertPost(request, response);
                    break;
                case "update":
                    updatePost(request, response);
                    break;
                case "addComment":
                    addComment(request, response);
                    break;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void listPosts(HttpServletRequest request, HttpServletResponse response) throws SQLException, ServletException, IOException {
        List<Post> posts = postService.getAllPosts();
        request.setAttribute("posts", posts);

        String view = request.getParameter("view");
        String page = "posts_manage/list-posts.jsp";
        if ("blog".equals(view)) {
            page = "blog.jsp";
        }

        RequestDispatcher dispatcher = request.getRequestDispatcher(page);
        dispatcher.forward(request, response);
    }

    private void showNewForm(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        List<Location> locations = locationService.getAllLocations();
        List<Category> categories = categoryService.getAllCategories();
        request.setAttribute("locations", locations);
        request.setAttribute("categories", categories);
        RequestDispatcher dispatcher = request.getRequestDispatcher("posts_manage/form-post.jsp");
        dispatcher.forward(request, response);
    }

    private void showEditForm(HttpServletRequest request, HttpServletResponse response) throws SQLException, ServletException, IOException {
        int postId = Integer.parseInt(request.getParameter("postId"));
        Post post = postService.getPost(postId);
        List<Comment> comments = commentService.getCommentsWithUserAndPost(postId);
        List<Location> locations = locationService.getAllLocations();
        List<Category> categories = categoryService.getAllCategories();

        request.setAttribute("post", post);
        request.setAttribute("comments", comments);
        request.setAttribute("locations", locations);
        request.setAttribute("categories", categories);
        RequestDispatcher dispatcher = request.getRequestDispatcher("posts_manage/form-post.jsp");
        dispatcher.forward(request, response);
    }

    private void insertPost(HttpServletRequest request, HttpServletResponse response) throws SQLException, IOException {
        String title = request.getParameter("title");
        String content = request.getParameter("content");
        String imageUrl = request.getParameter("imageUrl");

        String locationIdStr = request.getParameter("locationId");
        int locationId = (locationIdStr == null || locationIdStr.isEmpty()) ? 0 : Integer.parseInt(locationIdStr);

        String categoryIdStr = request.getParameter("categoryId");
        int categoryId = (categoryIdStr == null || categoryIdStr.isEmpty()) ? 0 : Integer.parseInt(categoryIdStr);

        HttpSession session = request.getSession();
        User loggedInUser = (User) session.getAttribute("user");
        int authorId = loggedInUser.getUserId();

        java.util.Date currentDate = new java.util.Date();

        Post newPost = new Post();
        newPost.setTitle(title);
        newPost.setContent(content);
        newPost.setImageUrl(imageUrl);
        newPost.setLocationId(locationId);
        newPost.setCategoryId(categoryId);
        newPost.setAuthorId(authorId);
        newPost.setCreatedAt(new java.sql.Date(currentDate.getTime()));

        if (locationId == 0 || categoryId == 0) {
            request.getSession().setAttribute("message", "Vui lòng chọn địa điểm và danh mục hợp lệ.");
            request.getSession().setAttribute("status", "error");
            response.sendRedirect("PostServlet?action=new");
        } else {
            postService.addPost(newPost);
            request.getSession().setAttribute("message", "Thêm bài viết thành công!");
            request.getSession().setAttribute("status", "success");
            response.sendRedirect("PostServlet?action=list");
        }
    }

    private void updatePost(HttpServletRequest request, HttpServletResponse response) throws SQLException, IOException {
        String postIdStr = request.getParameter("postId");
        int postId = (postIdStr == null || postIdStr.isEmpty()) ? 0 : Integer.parseInt(postIdStr);

        String title = request.getParameter("title");
        String content = request.getParameter("content");
        String imageUrl = request.getParameter("imageUrl");

        String locationIdStr = request.getParameter("locationId");
        int locationId = (locationIdStr == null || locationIdStr.isEmpty()) ? 0 : Integer.parseInt(locationIdStr);

        String categoryIdStr = request.getParameter("categoryId");
        int categoryId = (categoryIdStr == null || categoryIdStr.isEmpty()) ? 0 : Integer.parseInt(categoryIdStr);

        HttpSession session = request.getSession();
        User loggedInUser = (User) session.getAttribute("user");
        int authorId = loggedInUser.getUserId();

        java.util.Date currentDate = new java.util.Date();

        Post updatedPost = postService.getPost(postId);
        updatedPost.setTitle(title);
        updatedPost.setContent(content);
        updatedPost.setImageUrl(imageUrl);
        updatedPost.setLocationId(locationId);
        updatedPost.setCategoryId(categoryId);
        updatedPost.setAuthorId(authorId);
        updatedPost.setUpdatedAt(new java.sql.Date(currentDate.getTime()));

        if (locationId == 0 || categoryId == 0) {
            request.getSession().setAttribute("message", "Vui lòng chọn địa điểm và danh mục hợp lệ.");
            request.getSession().setAttribute("status", "error");
            response.sendRedirect("PostServlet?action=edit&postId=" + postId);
        } else {
            postService.updatePost(updatedPost);
            request.getSession().setAttribute("message", "Cập nhật bài viết thành công!");
            request.getSession().setAttribute("status", "success");
            response.sendRedirect("PostServlet?action=list");
        }
    }

    private void deletePost(HttpServletRequest request, HttpServletResponse response) throws SQLException, IOException {
        int postId = Integer.parseInt(request.getParameter("postId"));
        postService.deletePost(postId);
        request.getSession().setAttribute("message", "Xóa bài viết thành công!");
        request.getSession().setAttribute("status", "success");
        response.sendRedirect("PostServlet?action=list");
    }

    private void viewPost(HttpServletRequest request, HttpServletResponse response) throws SQLException, ServletException, IOException {
        int postId = Integer.parseInt(request.getParameter("postId"));
        Post post = postService.getPost(postId);
        List<Comment> comments = commentService.getCommentsWithUserAndPost(postId);

        request.setAttribute("post", post);
        request.setAttribute("comments", comments);
        RequestDispatcher dispatcher = request.getRequestDispatcher("posts_manage/detail-post.jsp");
        dispatcher.forward(request, response);
    }

    /*********  Comment  *********/
    private void deleteComment(HttpServletRequest request, HttpServletResponse response) throws SQLException, IOException {
        String action = request.getParameter("action");
        if ("deleteComment".equals(action)) {
            int commentId = Integer.parseInt(request.getParameter("commentId"));
            String postId = request.getParameter("postId");
            commentService.deleteComment(commentId);
            if (postId != null && !postId.isEmpty()) {
                request.getSession().setAttribute("message", "Đã xóa bình luận!");
                request.getSession().setAttribute("status", "success");
                response.sendRedirect("PostServlet?action=edit&postId=" + postId);
            } else {
                response.sendRedirect("PostServlet?action=list");
            }
        }
    }

    private void addComment(HttpServletRequest request, HttpServletResponse response) throws SQLException, IOException {
        String content = request.getParameter("content");
        int postId = Integer.parseInt(request.getParameter("postId"));

        HttpSession session = request.getSession();
        User loggedInUser = (User) session.getAttribute("user");
        if (loggedInUser != null) {
            int authorId = loggedInUser.getUserId();

            Comment comment = new Comment();
            comment.setContent(content);
            comment.setCreatedAt(new java.util.Date());
            comment.setUserId(authorId);
            comment.setPostId(postId);

            commentService.addComment(comment);
            request.getSession().setAttribute("message", "Thêm bình luận bài viết thành công!");
            request.getSession().setAttribute("status", "success");
            response.sendRedirect("PostServlet?action=edit&postId=" + postId);
        } else {
            response.sendRedirect("login.jsp");
        }
    }

    /*********  Category  *********/
    private void loadCategories(HttpServletRequest request) {
        List<Category> categories = categoryService.getAllCategories();
        request.setAttribute("categories", categories);
    }

    private void setCategories(HttpServletRequest request) {
        List<Category> categories = categoryService.getAllCategories();
        request.setAttribute("categories", categories);
    }

    private void listCategories(HttpServletRequest request, HttpServletResponse response)
            throws SQLException, ServletException, IOException {
        setCategories(request);
        RequestDispatcher dispatcher = request.getRequestDispatcher("postList.jsp");
        dispatcher.forward(request, response);
    }

    private void listPostsByCategory(HttpServletRequest request, HttpServletResponse response)
            throws SQLException, ServletException, IOException {
        String categoryId = request.getParameter("categoryId");
        List<Post> posts = postService.getPostsByCategory(Integer.parseInt(categoryId));
        request.setAttribute("posts", posts);

        RequestDispatcher dispatcher = request.getRequestDispatcher("category_posts.jsp"); // Trang hiển thị bài viết
        dispatcher.forward(request, response);
    }
}