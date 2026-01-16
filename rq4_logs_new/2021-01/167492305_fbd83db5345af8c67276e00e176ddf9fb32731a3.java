package william.oop.idgenerator;

/**
 * @Author zhangshenao
 * @Date 2021-01-21
 * @Description
 */
public class Client {
    public static void main(String[] args) {
        IdGenerator generator = new RandomIdGenerator();
        System.err.println(generator.generate());
    }
}