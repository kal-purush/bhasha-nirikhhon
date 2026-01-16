import java.util.*;

public class Laptop {
    private int id;
    private String model;
    private int mainMemory;
    private int hardDisk;
    private String processor;
    private String videoCard;
    private String operatingSystem;
    private Double display;
    private Double weight;
    private int price;

    public Laptop(int id, String model, int mainMemory, int hardDisk,
                  String processor, String videoCard, String operatingSystem,
                  Double display, Double weight, int price) {
        this.id = id;
        this.model = model;
        this.mainMemory = mainMemory;
        this.hardDisk = hardDisk;
        this.processor = processor;
        this.videoCard = videoCard;
        this.operatingSystem = operatingSystem;
        this.display = display;
        this.weight = weight;
        this.price = price;
    }

    public int getId() {

        return id;
    }

    public void setId(int id) {

        this.id = id;
    }

    public String getModel() {

        return model;
    }

    public void setModel(String model) {

        this.model = model;
    }

    public int getMainMemory() {


        return mainMemory;
    }

    public void setMainMemory(int mainMemory) {

        this.mainMemory = mainMemory;
    }

    public int getHardDisk() {

        return hardDisk;
    }

    public void setHardDisk(int hardDisk) {

        this.hardDisk = hardDisk;
    }

    public String getProcessor() {

        return processor;
    }

    public void setProcessor(String processor) {

        this.processor = processor;
    }

    public String getVideoCard() {

        return videoCard;
    }

    public void setVideoCard(String videoCard) {

        this.videoCard = videoCard;
    }

    public String getOperatingSystem() {

        return operatingSystem;
    }

    public void setOperatingSystem(String operatingSystem) {

        this.operatingSystem = operatingSystem;
    }

    public Double getDisplay() {

        return display;
    }

    public void setDisplay(Double display) {

        this.display = display;
    }

    public Double getWeight() {

        return weight;
    }

    public void setWeight(Double weight) {

        this.weight = weight;
    }

    public int getPrice() {

        return price;
    }

    public void setPrice(int price) {

        this.price = price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Laptop laptop = (Laptop) o;
        return id == laptop.id && mainMemory == laptop.mainMemory && hardDisk == laptop.hardDisk
                && Objects.equals(model, laptop.model) && Objects.equals(processor, laptop.processor)
                && Objects.equals(videoCard, laptop.videoCard) && Objects.equals(operatingSystem, laptop.operatingSystem)
                && Objects.equals(display, laptop.display) && Objects.equals(weight, laptop.weight);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, model);
    }

    @Override
    public String toString() {
        return "Laptop {" +
                "id: " + id +
                ", Модель: " + model +
                ", ОЗУ: " + mainMemory +
                ", Объем ЖД: " + hardDisk +
                ", Процессор: " + processor +
                ", Видеокарта: " + videoCard +
                ", Операционная система: " + operatingSystem +
                ", Дисплей: " + display +
                ", Вес: " + weight +
                ", Цена: " + price +
                '}';
    }

    public static Set<Laptop> createLaptopSet() {
        Set<Laptop> laptopSet = new HashSet<>();
        laptopSet.add(new Laptop(1, "model1", 1, 1, "processor1", "videoCard1",
                "operatingSystem1", 1.0, 1.0, 1));
        laptopSet.add(new Laptop(2, "model2", 2, 2, "processor2", "videoCard2",
                "operatingSystem2", 2.0, 2.0, 2));
        laptopSet.add(new Laptop(3, "model3", 1, 1, "processor1", "videoCard1",
                "operatingSystem1", 1.0, 1.0, 1));
        laptopSet.add(new Laptop(4, "model4", 4, 4, "processor4", "videoCard4",
                "operatingSystem4", 4.0, 4.0, 4));
        laptopSet.add(new Laptop(5, "model5", 5, 5, "processor5", "videoCard5",
                "operatingSystem5", 5.0, 5.0, 5));
        laptopSet.add(new Laptop(6, "model6", 1, 1, "processor1", "videoCard1",
                "operatingSystem1", 1.0, 1.0, 1));
        for (Laptop i : laptopSet) {
            System.out.println(i);
        }
        return laptopSet;
    }

    public static Map<String, String> enterFilter() {
        Map<String, String> filterMap = new HashMap<>();
        String key = "";
        while (!key.equals("q")) {
            System.out.printf("Введите цифру, соответствующую необходимому критерию:\n" +
                    "1 - Модель\n" +
                    "2 - ОЗУ\n" +
                    "3 - Объем ЖД\n" +
                    "4 - Процессор\n" +
                    "5 - Видеокарта\n" +
                    "6 - Операционная система\n" +
                    "7 - Дисплей\n" +
                    "8 - Вес\n" +
                    "9 - Цена\n" +
                    "q - Сохранить критерии и выйти\n" +
                    "-> ");
            Scanner scn = new Scanner(System.in);
            key = scn.nextLine();
            if (!key.equals("q")) {
                System.out.printf("Введите значение критерия:\n" +
                        "-> ");
                String value = scn.nextLine();
                filterMap.put(key, value);
            }
        }
        return filterMap;
    }

    public static boolean selectionByFilter(Laptop laptop, Map<String, String> filterMap) {
        Boolean result = false;
        Integer count = 0;
        for (String key : filterMap.keySet()) {
            if (key.equals("1")) {
                if (filterMap.get("1").equals(laptop.model)) {
                    count += 1;
                }
            }
            if (key.equals("2")) {
                int intValue = Integer.parseInt(filterMap.get("2"));
                if (intValue <= laptop.mainMemory) {
                    count += 1;
                }
            }
            if (key.equals("3")) {
                int intValue = Integer.parseInt(filterMap.get("3"));
                if (intValue <= laptop.hardDisk) {
                    count += 1;
                }
            }
            if (key.equals("4")) {
                if (filterMap.get("4").equals(laptop.processor)) {
                    count += 1;
                }
            }
            if (key.equals("5")) {
                if (filterMap.get("5").equals(laptop.videoCard)) {
                    count += 1;
                }
            }
            if (key.equals("6")) {
                if (filterMap.get("6").equals(laptop.operatingSystem)) {
                    count += 1;
                }
            }
            if (key.equals("7")) {
                double doubleValue = Double.parseDouble(filterMap.get("7"));
                if (doubleValue <= laptop.display) {
                    count += 1;
                }
            }
            if (key.equals("8")) {
                double doubleValue = Double.parseDouble(filterMap.get("8"));
                if (doubleValue <= laptop.weight) {
                    count += 1;
                }
            }
            if (key.equals("9")) {
                int intValue = Integer.parseInt(filterMap.get("9"));
                if (intValue <= laptop.price) {
                    count += 1;
                }
            }
        }
        if ((filterMap.keySet().size()) == count) {
            result = true;
        }
        return result;
    }

    public static void main(String[] args) {
        Set<Laptop> laptopSet = createLaptopSet();
        Map<String, String> filterMap = enterFilter();
        for (Laptop laptop : laptopSet) {
            if (selectionByFilter(laptop, filterMap)) {
                System.out.println(laptop);
            }
        }
    }
}