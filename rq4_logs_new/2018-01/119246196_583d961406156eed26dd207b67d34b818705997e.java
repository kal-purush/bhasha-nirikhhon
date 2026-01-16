package MusicDraw;

public class ColorPalette {

    public static int drawing_color = Main.parent.color(0);
    public static int select_color_num = 0;
    private static int px = 70, py = 400;

    static int[] colors = {
            Main.parent.color(234, 46, 51),
            Main.parent.color(9, 22, 21),
            Main.parent.color(347, 27, 99),
            Main.parent.color(61, 93, 99)
    };

    public static void draw() {
        Main.parent.noStroke();
        Main.parent.fill(UI.colorset.get("lightGray"));
        Main.parent.rect(px, py, 360, 300);
        for (int i = 0; i < colors.length; i++) {
            if (select_color_num == i){
                Main.parent.strokeWeight(2);
                Main.parent.stroke(0,90,90);
            }
            Main.parent.fill(colors[i]);
            Main.parent.rect(px + i * 60 + 60, py + 180, 55, 55);
            Main.parent.noStroke();
        }
        Main.parent.fill(drawing_color);
        Main.parent.rect(px + 70 + 60, py + 60, 100, 100);
    }

    public static float frec() {
        switch (select_color_num) {
            case 0:
                return 200;
            case 1:
                return 440;
            case 2:
                return 880;
            case 3:
                return 600;
            default:
                return 300;
        }
    }

    public static void mousePressed() {
        select_color_num = -1;
        for (int i = 0; i < colors.length; i++) {
            if (Main.parent.mouseX > px + i * 60 + 60 &&
                    Main.parent.mouseX < px + i * 60 + 115 &&
                    Main.parent.mouseY > py + 180 &&
                    Main.parent.mouseY < py + 180 + 55)
                select_color_num = i;
        }
        System.out.println(select_color_num);
    }

}