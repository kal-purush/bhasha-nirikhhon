package by.it_academy.team1.usermessages.endpoints.html;

import by.it_academy.team1.usermessages.core.dto.UserLoginDto;
import by.it_academy.team1.usermessages.core.entity.Message;
import by.it_academy.team1.usermessages.core.exceptions.UserNotFoundException;
import by.it_academy.team1.usermessages.service.api.IMessageService;
import by.it_academy.team1.usermessages.service.factory.MessageServiceFactory;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

import java.io.IOException;
import java.util.List;

@WebServlet(urlPatterns = "/ui/user/chats")
public class ChatsServlet  extends HttpServlet {

    public final IMessageService messageService;

    public ChatsServlet(){ this.messageService = MessageServiceFactory.getInstance();}
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        HttpSession session = req.getSession();

        try{
            UserLoginDto currentUser = (UserLoginDto) session.getAttribute("user");
            List<Message> messages = this.messageService.getMessagesOfUser(currentUser.getUsername());
            req.setAttribute("chat", messages);
            req.getRequestDispatcher("/template/ui/user/chats/").forward(req, resp);
        } catch (UserNotFoundException e){
            resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            resp.getWriter().write(e.getMessage());
        }
    }
}