import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// СЕГМЕНТАТОР_ТЕКСТА
// 1) получение искателем_строк массива значений для оцифровывания
// 2) с помощью метода границ искатель_строк делит массив значений изображения на "строки", которые определяют реальные строки текста с изображения
// 3) передача 'строки' в искатель_слов, а после передача в составитель_текста значения '999', определяющее '\n' (новую строку)
// 4) искатель_слов через сравнивание среднего значения яркости каждого столбца полученной 'строки' с общим средним значением яркости 'строки' делит массив на 'слова', определяющие реальные слова текста с изображения
// 5) передача 'слова' в искатель_символов, а после передача в составитель_текста значения '666', определяющее ' ' (пробел между словами)
// 6) искатель_символов, используя три этапа определения настоящих границ каждого символа, получает 'символы', определяющие реальные символы текста с изображения
// 7) отправка массива значений 'символа' в метод_создания_изображения
// 8) создается новое изображения символа
// 9) передача этого изображения в нейросеть
// ДОПОЛНИТЕЛЬНО:
// 10) выгрузка строк в папку "Save", полученных в искателе_строк, в виде изображений (для проверки)
// 11) выгрузка слов в папку "Save", полученных в искателе_слов, в виде изображений (для проверки)
// 12) выгрузка символов в папку "Save", полученных в искателе_символов, в виде изображений (для проверки)
// 13) возможность сегментатора отправлять полученные изображения вместо нейросети в обучатель_нейросети

public class Finder {
    NeuroNet neuronet = new NeuroNet();
    boolean isEmpty1 = true, isEmpty2 = true, isEmpty3 = true; // булеаны для пустых массивов, в которые сохраняются строки текста (нужны как условие записи искателями полученных ими значений)
    boolean whiteString = false; // нахождение конечной границы строки текста (нужны как условие записи искателями, полученных ими, значений)
    boolean analyzerString = false; // флаг работы искателя_строк (нужен для записи значений, полученных им)
    boolean analyzerWord = false; // флаг работы искателя_слов (нужен для записи значений, полученных им)
    boolean analyzerLetter = false; // флаг работы искателя_символов (нужен для записи значений, полученных им)
    double[][] pict; // временный массив
    int n = 0, nn = 0, nnn = 0; // временные переменные для создания новых изображений (для определения размеров полученного массива на выходе из каждого искателя)

    // ИСКАТЕЛЬ СТРОК
    public double[][] stringFind (double[][] pix, int height, int width) throws IOException {
        // включение искателя строк
        analyzerString = true;
        analyzerLetter = false;
        analyzerWord = false;

        pict = new double[height][width]; // идентифицируем размеры временного массива

        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                if (pix[i][j] >= 0.5) { // порог черного пикселя (находим стартовую границу строки текста)
                    whiteString = false;
                    zoner(pix, height, width, i); // запись одной строки пикселей
                    break;
                }
                else { // белый символ
                    whiteString = true;
                }
            }
            if ((!isEmpty1 & whiteString) | (!isEmpty1 & i == height - 1)) { // находим конечную границу строки текста
                double[][] newPict = new double[n][width]; // идентификация массива для полученной строки

                for (int q = 0; q < n; q++) {
                    for (int w = 0; w < width; w++) {
                        newPict[q][w] = pict[q][w]; // перезапись в новый массив
                    }
                }

                //imager(newPict, n, width); // выгрузка строк как изображений
                wordFind(newPict, n, width); // передача массива в искатель_слов

                neuronet.textFormer(999); // передача в составитель_текста информации о присутствии '\n'

                // устанавливаем значений по умолчанию
                isEmpty1 = true;
                whiteString = false;
                analyzerString = true;
                analyzerLetter = false;
                analyzerWord = false;
                n = 0;
                pict = new double[height][width];
            }
        }
        return pix;
    }

    // ИСКАТЕЛЬ СЛОВ
    public double[][] wordFind(double[][] pix, int height, int width) throws IOException {
        double[][] blur = new double[height][width]; // массив для хранения размытого изображения

        for (int i = 0; i < height; i++) { // размытие изображения (все соседние пиксели у черного пикселя устанавливаются как '1')
            for (int j = 0; j < width; j++) {
                if (pix[i][j] > 0.5) {
                    if (i != 0 & j != 0 & i != height - 1 & j != width - 1 & i != 1 & j != 1 & i != height - 2 & j != width - 2) {
                        blur[i][j] = 1;
                        blur[i + 1][j] = 1;
                        blur[i][j + 1] = 1;
                        blur[i - 1][j] = 1;
                        blur[i][j - 1] = 1;
                        blur[i + 1][j + 1] = 1;
                        blur[i - 1][j - 1] = 1;
                        blur[i + 1][j - 1] = 1;
                        blur[i - 1][j + 1] = 1;

                        blur[i][j + 2] = 1;
                        blur[i][j - 2] = 1;
                        blur[i + 2][j] = 1;
                        blur[i + 2][j + 1] = 1;
                        blur[i + 2][j + 2] = 1;
                        blur[i + 2][j - 1] = 1;
                        blur[i + 2][j - 2] = 1;
                        blur[i + 1][j + 2] = 1;
                        blur[i + 1][j - 2] = 1;
                        blur[i - 2][j] = 1;
                        blur[i - 2][j + 1] = 1;
                        blur[i - 2][j + 2] = 1;
                        blur[i - 2][j - 1] = 1;
                        blur[i - 2][j - 2] = 1;
                        blur[i - 1][j + 2] = 1;
                        blur[i - 1][j - 2] = 1;

                    }
                    // условия приграничных пикселей
                    if (i == 0) {
                        blur[i][j] = 1;
                        blur[i + 1][j] = 1;
                        if (j != 0) {
                            blur[i][j - 1] = 1;
                            blur[i + 1][j - 1] = 1;
                        }
                        if (j != width - 1) {
                            blur[i][j + 1] = 1;
                            blur[i + 1][j + 1] = 1;
                        }
                    }

                    if (i == height - 1) {
                        blur[i][j] = 1;
                        blur[i - 1][j] = 1;
                        if (j != 0) {
                            blur[i][j - 1] = 1;
                            blur[i - 1][j - 1] = 1;
                        }
                        if (j != width - 1) {
                            blur[i][j + 1] = 1;
                            blur[i - 1][j + 1] = 1;
                        }
                    }

                    if (j == 0) {
                        blur[i][j] = 1;
                        blur[i][j + 1] = 1;
                    }
                    if (j == width - 1) {
                        blur[i][j] = 1;
                        blur[i][j - 1] = 1;
                    }
                } else { // белые - в '0'
                    blur[i][j] = 0;
                }
            }
        }

        int sumCol = 0; // сумма пикселей в одном столбце
        double avrCol; // среднее значение пикселей в столбце
        double[] avrArr = new double[width]; // массив средних значений каждого столбца одной строки

        // находим средние значения яркости пикселей для каждого столбца в строке
        for (int q = 0; q < width; q++) {
            for (int w = 0; w < height; w++) {
                sumCol += blur[w][q];
            }
            avrCol = (double) sumCol / height;
            avrCol = Math.round(avrCol * 100) / 100.0;
            avrArr[q] = avrCol;
            sumCol = 0;
        }

        double average; // средний порог значений пикселей для полной одной строки
        double sumCol2 = 0;

        // находим среднее значение яркости пикселей для всей строки, которое будет являться порогом для средних значений каждого столбца
        for (int e = 0; e < avrArr.length; e++) {
            sumCol2 += avrArr[e];
        }
        average = sumCol2 / avrArr.length;
        average = Math.round(average * 100) / 100.0;

        int[] binaryZone = new int[avrArr.length]; // здесь храниятся индексы столбцов строки

        // сравниваем все полученные ранее значения яркости для каждого столбца со средним порогом яркости для всей строки
        for (int a = 0; a < avrArr.length; a++) {
            if (avrArr[a] >= average * 0.1) {
                binaryZone[a] = 1; // устанавливаем в '1', где находится слово
            } else {
                binaryZone[a] = 0;
            }
        }

        // включение искателя слов
        analyzerString = false;
        analyzerWord = true;
        analyzerLetter = false;

        pict = new double[height][width]; // идентифицируем размеры временного массива

        // запись нового изображения
        for (int i = 0; i < width; i++) {
            for (int j = 0; j < height; j++) {
                if (binaryZone[i] == 1) { // исходя из полученных данных предыдущих вычислений, записываем пиксели в новый массив
                    whiteString = false;
                    zoner(pix, height, width, i);
                    break;
                } else {
                    whiteString = true;
                }
            }
            if ((!isEmpty2 & whiteString) | (!isEmpty2 & i == width - 1)) { // находим конечную границу слова
                double[][] newPict = new double[height][nn]; // идентификация массива для нового слова

                for (int q = 0; q < height; q++) {
                    for (int w = 0; w < nn; w++) {
                        newPict[q][w] = pict[q][w]; // перезапись слова
                    }
                }

                //imager(newPict, height, nnn); // выгрузка слов как изображений
                letterFind(newPict, height, nn); // передача слова в искатель_символов

                neuronet.textFormer(666); // передача в составитель_текста информации о присутствии ' '

                // устанавливаем значения по умолчанию
                isEmpty2 = true;
                whiteString = false;
                analyzerString = false;
                analyzerWord = true;
                analyzerLetter = false;
                nn = 0;
                pict = new double[height][width];
            }
        }
        return pix;
    }

    // ИСКАТЕЛЬ СИМВОЛОВ
    public double[][] letterFind(double[][] pix, int height, int width) throws IOException {
        double t = 0.3 * height;
        int interval = (int) Math.round(t); // примерная ширина символа

        int sumCol = 0; // сумма пикселей в одном столбце
        double avrCol; // среднее значение пикселей в столбце
        double[] avrArr = new double[width]; // массив средних значений каждого столбца одного слова

        // находим средние значения яркости пикселей для каждого столбца в слове
        for (int q = 0; q < width; q++) {
            for (int w = 0; w < height; w++) {
                sumCol += pix[w][q];
            }
            avrCol = (double) sumCol / height;
            avrCol = Math.round(avrCol * 100) / 100.0;
            avrArr[q] = avrCol;
            sumCol = 0;
        }

        double average; // средний порог значений пикселей для полного одного слова
        double sumCol2 = 0;

        // находим среднее значение яркости пикселей для всего слова
        for (int e = 0; e < avrArr.length; e++) {
            sumCol2 += avrArr[e];
        }
        average = sumCol2 / avrArr.length;
        average = Math.round(average * 100) / 100.0;
        average = average * 0.1;

        List<Integer> gran = new ArrayList<>(); // список межсимвольных границ
        double min = avrArr[0]; // переменная минимума для нахождения межсимвольных границ
        int index = 0; // индекс найденных минимумов
        int s = 0; // переменная для сдвига

        // находим минимальное среднее (min) значение столбца на отрезке (0, interval)
        // далее находим на отрезке от (min+1, interval+1)
        // и так далее до конечной границы слова
        // таким образом получаем группу предварительных межсимвольных границ
        while (interval + s < avrArr.length) {
            for (int q = s; q < interval + s; q++) {
                if (avrArr[q] < min) {
                    min = avrArr[q];
                    index = q;
                }
            }
            gran.add(index);
            s = index + 1;
            index = s;
            min = avrArr[s];
        }
        for (int q = s; q < avrArr.length; q++) { // этот цикл нужен для прохода последнего отрезка
            if (avrArr[q] < min) {
                min = avrArr[q];
                index = q;
            }
        }
        gran.add(index);

        // первый шаг удаления лишних границ
        for (int q = 2; q < gran.size() - 2; q++) {
            int m = gran.get(q);
            if ((avrArr[m] < average) & ((avrArr[m - 2] > average) | (avrArr[m + 2] > average))) { // условие ложной границы
                Iterator<Integer> iter = gran.iterator();
                while (iter.hasNext()) {
                    Integer next = iter.next();
                    if (next.equals(q)) {
                        iter.remove();
                    }
                }
            }
        }

        // делим изображение слова на три уровня
        t = 0.3 * height;
        int upp = (int) Math.round(t); // верхний - 30% от высоты
        t = 0.4 * height;
        int mid = (int) Math.round(t); // средний - 40%
        int low = height - upp - mid; // нижний - 30%
        boolean bool1 = false, bool2 = false, bool3 = false; // булеаны для каждого условия удаления последних ложных межсимвольных границ

        // максимальные среди средних значений яркости для каждого уровня (текущий столбец, следующий, предыдущий)
        double maxU = 0, maxU_next = 0, maxU_prev = 0;
        double maxM = 0, maxM_next = 0, maxM_prev = 0;
        double maxL = 0, maxL_next = 0, maxL_prev = 0;

        // следующий шаг удаления лишних границ
        for (int q = 1; q < width - 1; q++) {
            for (int w = 0; w < height; w++) { // находим максимальные значения для каждого столбца и соседних ему на каждом уровне
                if (w <= upp) {
                    if (maxU < pix[w][q]) {
                        maxU = pix[w][q];

                    }
                    if (maxU_next < pix[w][q + 1]) {
                        maxU_next = pix[w][q + 1];

                    }
                    if (maxU_prev < pix[w][q - 1]) {
                        maxU_prev = pix[w][q - 1];

                    }
                }
                if ((w > upp) & (w < low)) {
                    if (maxM < pix[w][q]) {
                        maxM = pix[w][q];

                    }
                    if (maxM_next < pix[w][q + 1]) {
                        maxM_next = pix[w][q + 1];

                    }
                    if (maxM_prev < pix[w][q - 1]) {
                        maxM_prev = pix[w][q - 1];

                    }
                }
                if (w >= low) {
                    if (maxL < pix[w][q]) {
                        maxL = pix[w][q];

                    }
                    if (maxL_next < pix[w][q + 1]) {
                        maxL_next = pix[w][q + 1];

                    }
                    if (maxL_prev < pix[w][q - 1]) {
                        maxL_prev = pix[w][q - 1];

                    }
                }

            }
            if ((maxU == maxU_next) | (maxM == maxM_next) | (maxL == maxL_next) | (maxU == maxU_prev) | (maxM == maxM_prev) | (maxL == maxL_prev)) {
                bool1 = true; // если есть последовательные черные символы, то это ложная граница
            }

            // нахождения максимума для полных соседних столбцов
            if (maxU_next > maxM_next & maxU_next > maxL_next) {
                maxU_next = maxU_next;
            }
            if (maxL_next > maxM_next & maxU_next < maxL_next) {
                maxU_next = maxL_next;
            }
            if (maxU_next < maxM_next & maxM_next > maxL_next) {
                maxU_next = maxM_next;
            }
            if (maxU_prev > maxM_prev & maxU_prev > maxL_prev) {
                maxU_prev = maxU_prev;
            }
            if (maxL_prev > maxM_prev & maxU_prev < maxL_prev) {
                maxU_prev = maxL_prev;
            }
            if (maxU_prev < maxM_prev & maxM_prev > maxL_prev) {
                maxU_prev = maxM_prev;
            }

            // если среднее значение текущего столбца меньше максимального соседних, то это ложная граница
            if ((avrArr[q] < maxU_next) & (avrArr[q] < maxU_prev)) {
                bool2 = true;
            }

            // нахождение максимума для текущего полного столбца
            if (maxU > maxM & maxU > maxL) {
                maxU = maxU;
            }
            if (maxL > maxM & maxU < maxL) {
                maxU = maxL;
            }
            if (maxU < maxM & maxM > maxL) {
                maxU = maxM;
            }

            // третье условие для ложных границ
            double a_next = 2 * Math.abs(maxU - maxU_next);
            double a_prev = 2 * Math.abs(maxU - maxU_prev);
            if ((maxU > a_next) & (maxU > a_prev)) {
                bool3 = true;
            }

            // при срабатывании всех трех условий, граница удаляется
            if (bool1 & bool2 & bool3) {
                Iterator<Integer> iter = gran.iterator();
                while (iter.hasNext()) {
                    Integer next = iter.next();
                    if (next.equals(q)) {
                        iter.remove();
                    }
                }
            }

            // обнуляем переменные
            bool1 = false;
            bool2 = false;
            bool3 = false;
            maxU = 0;
            maxU_next = 0;
            maxU_prev = 0;
            maxM = 0;
            maxM_next = 0;
            maxM_prev = 0;
            maxL = 0;
            maxL_next = 0;
            maxL_prev = 0;
        }

        // записываем в массив финальную группу межсимвольных границ
        double power = 0.4 * height; // минимальное расстояние между границами
        int count = 0; // счетчик расстояний между границами
        int[] binaryZone = new int[avrArr.length]; // здесь хранятся индексы столбцов слова

        for (int a = 0; a < avrArr.length; a++) {
            if (gran.contains(a) & (count > power)) { // границы не должны быть слишком близко
                binaryZone[a] = 0;
                count = 0;
            }
            else {
                binaryZone[a] = 1;
                count++;
            }
        }

        // включение искателя символов
        analyzerString = false;
        analyzerWord = false;
        analyzerLetter = true;
        pict = new double[height][width]; // идентифицируем размеры временного массива

        // запись нового изображения
        for (int i = 0; i < width; i++) {
            for (int j = 0; j < height; j++) {
                if (binaryZone[i] == 1) { // исходя из полученных данных предыдущих вычислений записываем пиксели в новый массив
                    whiteString = false;
                    zoner(pix, height, width, i);
                    break;
                }
                else {
                    whiteString = true;
                }
            }
            if ((!isEmpty3 & whiteString) | (!isEmpty3 & i == width - 1)) { // записываем конечную границу символа
                double[][] newPict = new double[height][nnn]; // идентификация массива для нового символа

                for (int q = 0; q < height; q++) {
                    for (int w = 0; w < nnn; w++) {
                        newPict[q][w] = pict[q][w]; // перезапись символа
                    }
                }
                if (nnn > 3) { // используем массивы с шириной больше '3'
                    imager(newPict, height, nnn); // создание изображения символа
                }

                // устанавливаем значения по умолчанию
                isEmpty3 = true;
                whiteString = false;
                analyzerString = false;
                analyzerWord = false;
                analyzerLetter = true;
                nnn = 0;
                pict = new double[height][width];
            }
        }
        return pix;
    }

    // ЗАПИСЬ СТРОК МАССИВОВ ДЛЯ КАЖДОГО ИСКАТЕЛЯ
    public void zoner(double[][] pix, int height, int width, int i) {

        if (analyzerString) { // для искателя строк
            for (int j = 0; j < width; j++) {
                pict[n][j] = pix[i][j];
            }
            isEmpty1 = false; // теперь массив не пустой
            n++;
        }
        if (analyzerWord) { // для искателя слов
            for (int j = 0; j < height; j++) {
                pict[j][nn] = pix[j][i];
            }
            nn++;
            isEmpty2 = false; // теперь массив не пустой
        }
        if (analyzerLetter) { // для искателя символов
            for (int j = 0; j < height; j++) {
                pict[j][nnn] = pix[j][i];
            }
            nnn++;
            isEmpty3 = false; // теперь массив не пустой
        }
    }

    int sc = 1; // счетчик файлов

    // ПРЕОБРАЗОВАНИЕ ПОЛУЧЕННОГО МАССИВА ИЗ ИСКАТЕЛЯ_СИМВОЛОВ В ИЗОБРАЖЕНИЕ И ОТПРАВКА В НЕЙРОСЕТЬ
    public void imager (double[][] pixels, int n, int w) throws IOException {
        BufferedImage img = new BufferedImage(w, n, BufferedImage.TYPE_INT_RGB);

        String string = "src/Save/img" + sc + ".png";
        File f = new File(string);

        int[] im = new int[n * w]; // массив со значениями пикселей, на основе которого будет создаваться изображение
        int i = 0;

        for (int y = 0; y < n; y++) { // заполнение массива
            for (int x = 0; x < w; x++) {
                double temp = (1 - pixels[y][x]) * 100 * 255;
                int zap = (int) temp;
                int r = zap & 0xff;
                int g = zap & 0xff;
                int b = zap & 0xff;

                im[i] = (r << 16) | (g << 8) | b; // кодировка пикселя в тип RGB
                img.setRGB(x, y, im[i]);
                i++;
            }
        }
        if (analyzerLetter){ // если изображение - символ, то передается в нейросеть
            neuronet.preporation(img); // передача изображения в нейросеть
            //neuronet.trainer(img); // передача изображения в обучатель
        }

        //ImageIO.write(img, "PNG", f); // выгрузка изображений

        sc++;
    }
}