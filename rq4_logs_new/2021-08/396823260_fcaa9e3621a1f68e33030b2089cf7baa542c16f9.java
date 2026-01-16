package homework.lab2;

import lombok.Data;

@Data
public class Client {
    private final int debt;
    private final int bonus;
    private final String name;
    private final int salary;

    private Client (int debt, int bonus, String name, int salary) {
        this.debt = debt;
        this.bonus = bonus;
        this.name = name;
        this.salary = salary;
    }

    public static class ClientBuilder {
        private int debt;
        private int bonus;
        private String name;
        private int salary;

        public ClientBuilder setDebt(int debt) {
            this.debt = debt;

            return this;
        }

        public ClientBuilder setBonus(int bonus) {
            this.bonus = bonus;

            return this;
        }

        public ClientBuilder setName(String name) {
            this.name = name;

            return this;
        }

        public ClientBuilder setSalary(int salary) {
            this.salary = salary;

            return this;
        }

        public Client build() {
            return new Client(debt, bonus, name, salary);
        }
    }
}