package tracker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Notifications {
    private static Notifications instance = null;

    private final List<Notification> notifications = new ArrayList<>();

    private Notifications() {
        instance = this;
    }

    public int getStudentsCount(List<Notification> list) {
        return list.stream()
                .map(Notification::getStudent)
                .collect(Collectors.toSet()).size();
    }

    public static Notifications manager() {
        return instance == null ? new Notifications() : instance;
    }

    public void addGraduation(Student student, Course course) {
        notifications.add(new Notification(student, course));
    }

    public List<Notification> getNotifications() {
        List<Notification> result = new ArrayList<>(notifications);
        notifications.clear();
        return result;
    }
}

class Notification {
    private final String message;
    private final Student student;

    public Notification(Student student, Course course) {
        this.student = student;
        message = "To: " + student.getEmail() + System.lineSeparator() +
                "Re: Your Learning Progress" + System.lineSeparator() +
                String.format("Hello, %s! You have accomplished our %s course!",
                        student.getFullName(), course.getName());
    }

    public Student getStudent() {
        return student;
    }

    @Override
    public String toString() {
        return message;
    }
}