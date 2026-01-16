package by.dutov.jee.service.fasade;

import by.dutov.jee.people.Person;
import by.dutov.jee.utils.AppUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.InternalResourceView;

import javax.servlet.http.HttpSession;

@Slf4j
@Service
@RequiredArgsConstructor
public class LoginService {
    private final CheckingService checkingService;

    public ModelAndView getLoginedUser(HttpSession session, ModelAndView modelAndView, String userName, String password) {
        InternalResourceView internalResourceView = new InternalResourceView();
        Person person = checkingService.checkUser(userName);
        boolean checkPass = false;
        if (person != null) {
            checkPass = checkingService.checkPassword(person, password);
        }
        final String errorMessage = "errorMessage";
        if (!checkPass || AppUtils.getLoginedUser(session) != null) {
            log.info("person == null or already logged in");
            modelAndView.addObject(errorMessage, "Invalid userName or password");
            internalResourceView.setAlwaysInclude(true);
            modelAndView.setView(internalResourceView);
            modelAndView.setViewName("loginPage");
            modelAndView.setStatus(HttpStatus.NOT_FOUND);
            return modelAndView;
        }
        log.info("successful login");
        AppUtils.storeLoginedUser(session, person);
        modelAndView.setViewName("main/homePage");
        modelAndView.setStatus(HttpStatus.OK);
        return modelAndView;
    }
}