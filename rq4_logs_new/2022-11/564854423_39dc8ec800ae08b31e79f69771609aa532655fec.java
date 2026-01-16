public class Main {
    public static void main(String[] args) {


        //Задание№1
        int int0 = 1234;
        byte byte0 = 123;
        short short0 = 12345;
        long long0 = 12345678910l;
        float float0 = 1.2f;
        double double0 = 1.234;

        System.out.println("Зачение переменной int0 с типом int равно " + int0);
        System.out.println("Зачение переменной byte0 с типом byte равно " +byte0);
        System.out.println("Зачение переменной short0 с типом short равно " + short0);
        System.out.println("Зачение переменной long0 с типом long равно " + long0);
        System.out.println("Зачение переменной float0 с типом float равно " + float0);
        System.out.println("Зачение переменной double0 с типом doubleравно " + double0 );





        //Задание №3
        int ludmilaPawlovna = 23;
        int annaSergeevna = 27;
        int ekaterinaAndreevna = 30;
        int totalSheet = 480;

        int sheetOneStudent = totalSheet / (ludmilaPawlovna + annaSergeevna + ekaterinaAndreevna);

        System.out.println("На каждого ученика рассчитано " + sheetOneStudent + " листов бумаги");

        //Задание №4
        int outputMin = 18 / 2;
        int output20Min = outputMin * 20;
        int outputDay = outputMin * 1440;
        int output3Day = outputDay * 3;
        int outputMon = outputDay * 30;

        System.out.println("За 20 минут машина произвела бутылок " + output20Min + " штук");
        System.out.println("За 1 сутки машина произвела бутылок " + outputDay + " штук");
        System.out.println("За 3 суток машина произвела бутылок " + output3Day + " штук");
        System.out.println("За месяц машина произвела бутылок " + outputMon + " штук");

        //Задание №5
        int totalJar = 120;
        int whiteJarCabinet = 2;
        int brownJarCabinet = 4;

        int totaljarCabinet = whiteJarCabinet + brownJarCabinet;
        int totalCabinet = totalJar / totaljarCabinet;
        int totalWhiteJar = whiteJarCabinet * totalCabinet;
        int totalBrownJar = brownJarCabinet * totalCabinet;
        System.out.println("В школе, где " + totalCabinet + " классов, нужно " + totalWhiteJar + " банок белой краски и " + totalBrownJar + " банок коричневой краски");


        //Задание №6
        int banana = 80;
        int milk = 105; // Грамм в 100мл
        int iceCream = 100;
        int egg = 70;

        int breakfast = banana * 5 + milk * 2 + iceCream * 2 + egg * 4;
        double breakfastKG = breakfast / 1000;
        System.out.println("Общий вес завтрака " + breakfastKG + " кг");



        //Задание №7
        int totalExcessWeigh = 7;
        double minWeighLoss = 0.25;
        double maxWeighLoss = 0.5;
        double amountMinDay = totalExcessWeigh / minWeighLoss;
        double amountMaxDay = totalExcessWeigh / maxWeighLoss;
        double amountMeanDay = (amountMinDay + amountMaxDay) / 2;


        System.out.println("Минимальное количество дней похудения " + amountMinDay);
        System.out.println("Максимальное количество дней похудения " + amountMaxDay);
        System.out.println("Среднее количество дней похудения " + amountMeanDay);


        //Задание №8
        double masha = 67760;
        double denis = 83690;
        double kristina = 76230;
        double mashaYear = masha * 12;
        double denisYear = denis * 12;
        double kristinaYear= kristina * 12;

        masha += masha * 0.1;
        denis += denis * 0.1;
        kristina += kristina * 0.1;
        mashaYear = masha * 12 - mashaYear;
        denisYear = denis * 12 - denisYear;
        kristinaYear = kristina * 12 - kristinaYear;

        System.out.println("Маша теперь получает " + masha + " рублей. Годовой доход вырос на " + mashaYear + " рублей.");
        System.out.println("Денис теперь получает " + denis + " рублей. Годовой доход вырос на " + denisYear + " рублей.");
        System.out.println("Кристина теперь получает " + kristina + " рублей. Годовой доход вырос на " + kristinaYear + " рублей.");
    }
}