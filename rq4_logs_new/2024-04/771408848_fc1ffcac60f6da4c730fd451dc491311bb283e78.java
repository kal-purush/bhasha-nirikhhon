class TP2_1_H071231005 {
    private String name;
    private int age;
    private boolean isMale;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getAge() {
        return age;
    }

    public void setGender(boolean isMale) {
        this.isMale = isMale;
    }

    public String getGender() {
        return isMale? "Laki-laki" : "Perempuan";
    }

    public static void main(String[] args) {
        TP2_1_H071231005 person = new TP2_1_H071231005();

        person.setName("Nurul Wahdania");
        person.setAge(18);
        person.setGender(false);

        System.out.println("=====Informasi yang Diperoleh=====");
        System.out.println("Nama        : " + person.getName());
        System.out.println("Usia        : " + person.getAge());
        System.out.println("Gender      : " + person.getGender());
    }
}