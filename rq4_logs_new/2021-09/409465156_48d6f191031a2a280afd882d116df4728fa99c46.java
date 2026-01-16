package app.springboot.utility.MysqlDb;


import app.springboot.utility.MysqlDb.Schema.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

@Service
public class AllSchema {

    @Autowired(required = false)
    public UserNotificationInterface UserNotification;

    public AllSchema() {
    }
}