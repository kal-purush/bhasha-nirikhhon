import java.time.LocalDate;

public class SeizaUranaiAll {
    public static void main(String[] args) {String[] luckyItems = {
    "ペン", "スマホ", "手帳", "鍵", "財布", "イヤリング", "時計", "マグカップ", "ハンカチ", "本"
};

LocalDate today = LocalDate.now();

System.out.println("🌟 " + today + " の星座占い 🌟\n");

for (String seiza : seizaList) {
    String key = today.toString() + seiza;

    int resultIndex = Math.abs((key + "result").hashCode()) % results.length;
    int colorIndex = Math.abs((key + "color").hashCode()) % luckyColors.length;
    int itemIndex = Math.abs((key + "item").hashCode()) % luckyItems.length;

    System.out.println("【 " + seiza + " 】");
    System.out.println("運勢: " + results[resultIndex]);
    System.out.println("ラッキーカラー: " + luckyColors[colorIndex]);
    System.out.println("ラッキーアイテム: " + luckyItems[itemIndex]);
    System.out.println();
}
}
}