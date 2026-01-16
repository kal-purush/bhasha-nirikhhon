public class Application {
    /**
     * This class contains a main method that allows you to manually test the HardcodedLogin challenge functionality.
     * No changes you make here affect the test case results. You must still run & pass the test cases.
     * All implementation code should be written in the Lab class.
     *
     * The Lab.login method should return true if a login should be permitted given two values.
     */
    public static void main(String[] args) {
        Lab loginLab = new Lab();
        System.out.println("Should 'admin' be allowed to login with 'qwerty'? (should be true):");
        System.out.println(loginLab.login("admin", "qwerty"));
        System.out.println("Should 'admin' be allowed to login with 'pass123'? (should be false):");
        System.out.println(loginLab.login("admin", "pass123"));
        System.out.println("Should 'user' be allowed to login with 'password'? (should be true):");
        System.out.println(loginLab.login("user", "password"));
        System.out.println("Should 'user' be allowed to login with 'mypass'? (should be false):");
        System.out.println(loginLab.login("admin", "mypass"));
    }
}