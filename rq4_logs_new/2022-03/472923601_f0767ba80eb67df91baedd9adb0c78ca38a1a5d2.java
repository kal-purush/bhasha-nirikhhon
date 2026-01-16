package empie.pie.NewSpringCourse.game;

import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component
@Primary
public class PacManGame implements GamingConsole {

    @Override
    public void up() {
        System.out.println("north");
    }
    @Override
    public void down() {
        System.out.println("south");
    }
    @Override
    public void left() {
        System.out.println("west");
    }
    @Override
    public void right() {
        System.out.println("east");
    }
}