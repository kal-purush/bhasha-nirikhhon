package janken;

import java.util.Random;

import javafx.scene.control.Label;

public class JankenController {

     private final Random random = new Random();
     private  static final String[] hands = {
         "グー", "チョキ", "パー"
     };

      private Label cpuHandLabel;
      private Label myHandLabel;
      private Label resultLabel;
      private Label scoreLabel;
      //コンストラクタ
      //引数  3つのラベル
      public JankenController(Label cpuHandLabel, Label myHandLabel,
        Label resultLabel,Label scoreLabel){
       this.cpuHandLabel = cpuHandLabel;
       this.myHandLabel = myHandLabel;
       this.resultLabel = resultLabel;
       this.scoreLabel = scoreLabel;

      }
     // 自分の手をグーとしてじゃんけんをするメソッド
       public void putOutRockHand() {
         doJanken(0);
     }

    // 自分の手をチョキとしてじゃんけんをするメソッド
     public void putOutScissorsHand() {
         doJanken(1);
     }

  // 自分の手をパーとしてじゃんけんをするメソッド
   public void putOutPaperHand() {
         doJanken(2);
     }

   //ラベルの更新メソッドであるupdateLabels()を呼び出し
   //自分の手(myHand)と相手の手(cpuHand)を渡す
   //CPUの手はgetCpuHand()で取得
     private void doJanken(int myHand) {
         updateLabels(myHand, getCpuHand());
     }
   //CPUの手
     private int getCpuHand() {
         return random.nextInt(3);
     }
     //じゃんけんの結果の取得
     private String getResult(int myHand, int cpuHand) {
         int r = (myHand - cpuHand + 3) % 3;

         return (r == 2) ? "あなたの勝ち！" : ((r == 1) ? "あなたの負け！" : "あいこ！");
     }
     //自分の手・相手の手・じゃんけんの結果を各ラベルに表示
     private void updateLabels(int myHand, int cpuHand) {
         cpuHandLabel.setText("コンピュータの手: " + hands[cpuHand]);
         myHandLabel.setText("あなたの手: " + hands[myHand]);
         resultLabel.setText("結果: " + getResult(myHand, cpuHand));
     }
}