package pl.khamul.handworkshop.service;

import org.springframework.stereotype.Service;
import pl.khamul.handworkshop.entity.User;
import pl.khamul.handworkshop.repository.OrderHistoryRepository;
import pl.khamul.handworkshop.repository.UserRepository;

import javax.servlet.http.HttpServletRequest;
import java.security.Principal;
import java.util.List;


@Service
public class OrderHistoryService {
    private OrderHistoryRepository orderHistoryRepository;
    private UserRepository userRepository;

    public OrderHistoryService(OrderHistoryRepository orderHistoryRepository, UserRepository userRepository) {
        this.orderHistoryRepository = orderHistoryRepository;
        this.userRepository = userRepository;
    }

    public List showHistory(HttpServletRequest request) {
        Principal principal = request.getUserPrincipal();
        User user = userRepository.findFirstByEmail(principal.getName());

        List orderHistory  = orderHistoryRepository.findHistoryByUserId(user.getId());
    return orderHistory;
    }
}