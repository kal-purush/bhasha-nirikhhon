import javax.swing.JFrame;

public class Main {
   public static void main(String[] args) {
      Game game = new Game(); // �깉濡쒖슫 寃뚯엫 媛앹껜 �깮�꽦
      JFrame frame = new JFrame(); // Jframe �깮�꽦
      frame.setTitle(Game.TITLE); // �봽�젅�엫�쓽 �젣紐⑹쓣 ���씠��濡� 蹂�寃�
      frame.add(game); // 留뚮뱺 game 媛앹껜瑜� 留뚮뱾�뼱吏� 李� �쐞�뿉 遺숈씤�떎
      frame.setResizable(false); // 李쎌“�젅�씠 遺덇��뒫�븯寃� �꽕�젙
      frame.pack(); // 留뚮뱾�뼱吏� 而댄룷�꼳�듃�뱾�쓣 李쎌뿉 留욊쾶 �떎 �빀爾먯쨲
      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      frame.setLocationRelativeTo(null);

      frame.setVisible(true);
      game.start(); // �뒪�젅�뱶 �떆�옉
   }
}