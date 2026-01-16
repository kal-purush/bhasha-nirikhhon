public abstract class Hogwarts {
    private String name;
    private int magic;
    private int transgession;

    public Hogwarts(String name, int magic, int transgession) {
        this.name = name;
        this.magic = magic;
        this.transgession = transgession;
    }

    public String getName() {
        return name;
    }

    public int getMagic() {
        return magic;
    }

    public int getTransgession() {
        return transgession;
    }

    abstract int calculateSpecificScore();

    abstract void printCompareOfStudents(Hogwarts best, Hogwarts worst);

    private int calculateGeneralScore() {
        return this.magic + this.transgession;
    }

    public void compare(Hogwarts hogwarts) {
        if (this.getClass().equals(hogwarts.getClass())) {
            this.compareBySpecific(hogwarts);
        } else {
            this.compareByGeneral(hogwarts);
        }
    }

    private void compareBySpecific(Hogwarts hogwarts) {
        int thisScore = this.calculateGeneralScore() + this.calculateSpecificScore();
        int otherScore = hogwarts.calculateGeneralScore() + hogwarts.calculateSpecificScore();

        if (thisScore > otherScore) {
            printCompareOfStudents(this, hogwarts);
        } else if (thisScore < otherScore) {
            printCompareOfStudents(hogwarts, this);
        } else {
            System.out.println("Ученики равны по силе");
        }
    }

    private void compareByGeneral(Hogwarts hogwarts) {
        int thisScore = this.calculateGeneralScore();
        int otherScore = hogwarts.calculateGeneralScore();

        if (thisScore > otherScore) {
            System.out.println(String.format("Ученик %s сильнее ученика %s", this.name, hogwarts.getName()));
        } else if (thisScore < otherScore) {
            System.out.println(String.format("Ученик %s сильнее ученика %s", hogwarts.getName(), this.name));
        } else {
            System.out.println("Ученики равны по силе");
        }
    }

    @Override
    public String toString() {
        return "Hogwarts{" +
                "name" + name + '\'' +
                "Magic" + magic +
                "Transgression" + transgession +
                "}";
    }

}