package com.company.getyourgoal.service;

import com.company.getyourgoal.model.Comment;
import com.company.getyourgoal.model.Goal;
import com.company.getyourgoal.model.User;
import com.company.getyourgoal.repository.CommentRepository;
import com.company.getyourgoal.repository.GoalRepository;
import com.company.getyourgoal.repository.UserRepository;
import com.company.getyourgoal.viewmodel.UserViewModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.transaction.Transactional;
import java.util.List;
import java.util.Optional;

@Component
public class ServiceLayer {

    UserRepository userRepository;
    GoalRepository goalRepository;
    CommentRepository commentRepository;

    @Autowired

    public ServiceLayer(UserRepository userRepository,
                        GoalRepository goalRepository,
                        CommentRepository commentRepository) {
        this.userRepository = userRepository;
        this.goalRepository = goalRepository;
        this.commentRepository = commentRepository;
    }

    @Transactional
    public UserViewModel saveUser(UserViewModel viewModel) throws Exception {

        User u = new User();
        u.setFirstName(viewModel.getFirstName());
        u.setLastName(viewModel.getLastName());
        u.setUsername(viewModel.getUsername());
        u.setEmail(viewModel.getEmail());
        u.setPassword(viewModel.getPassword());
        u.setGoals(viewModel.getGoals(goal.get()));
        u.setComments(viewModel.getComments());

        u= userRepository.save(u);
        viewModel.setId(u.getId());

        List<Goal> goals = viewModel.getGoals(goal.get());
        goals.stream()
                .forEach(g -> {
                    g.setUserId(viewModel.getId());
                    goalRepository.save(g);
                });
        goals = goalRepository.findAllGoalsByUserId(viewModel.getId());
        viewModel.setGoals(goals);

        return viewModel;
    }

    public UserViewModel findOneUser(int id) {
        Optional<User> user = userRepository.findById(id);
        return user.isPresent() ? builUserViewModel(user.get()): null;
    }

    private UserViewModel builUserViewModel(User user) {
        Optional<Goal> goal =goalRepository.findById(user.getGoals());

        Optional<Comment> comment = commentRepository.findById(user.getComments());

        UserViewModel uvm = new UserViewModel();
        uvm.setId(user.getId());
        uvm.setFirstName(user.getFirstName());
        uvm.setLastName(user.getLastName());
        uvm.setUsername(user.getUsername());
        uvm.setEmail(user.getEmail());
        uvm.setPassword(user.getPassword());
        uvm.setGoals(goalList);
        uvm.setComments(commentList);



    }
}