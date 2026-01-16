package task_6;

/**
 * 1) Создайте абстрактный класс, который определяет базовую ставку зарплаты для сотрудников фирмы.
 * 2) Создайте подклассы данного класса для детализации заработной платы для различных видов сотрудников.
 * ...Пусть зарплата программиста составляет 4 базовых ставки,
 * ...а зарплата экономиста – 2 базовых ставки,
 * ...а коэффициент зарплаты директора равен 10.
 */

public abstract class BaseSalary {

    public static final double BASE_SALARY = 10_000.0;
    public static final double MULTIPLAING_SALARY_DIRECTOR = 10.0;
    public static final double MULTIPLAING_SALARY_CODER = 4.0;
    public static final double MULTIPLAING_SALARY_ECONOMIST = 2.0;

    public abstract double getSalary();

}