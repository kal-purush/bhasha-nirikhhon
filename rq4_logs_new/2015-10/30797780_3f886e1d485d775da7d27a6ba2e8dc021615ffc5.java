package puyo.server;

import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import javax.swing.JButton;
import javax.swing.JLabel;

import com.google.gson.Gson;

import puyo.data.Box;
import puyo.data.Game;
import puyo.data.Puyo;
import puyo.data.PuyoState;
import puyo.data.Box.BoxState;
import puyo.server.PuyoPuyoMaster.PuyoEx;

public class Puyofu {
	static Gson gson = new Gson();
	
	static JButton backBtn;
	static JLabel posLabel;
	static JButton nextBtn;

	// 現在位置
	static int pos = -1;

	// name
	static String[] name = new String[2];
	
	public static void main(String[] args) throws IOException {
		// 引数からぷよ譜のファイルパスを取得
		if (args.length < 1) {
			System.out.println("arg1 : puyofu file path");
			return;
		}
		String filepath = args[0];
		
		// ぷよ譜読み込み
		BufferedReader br = new BufferedReader(new FileReader(filepath));
		String strLine;
		ArrayList<String> puyofuList = new ArrayList<String>();
		while((strLine = br.readLine()) != null) {
			if (strLine.length() > 1 && strLine.charAt(0) == '{') {
				puyofuList.add(strLine);
			}
		}
		br.close();
		
		PuyoPain canvas = new PuyoPain(500, 400);
		PuyoPuyoMaster master = new PuyoPuyoMaster();
		
		// コントロール作成
		backBtn = new JButton("back");
		posLabel = new JLabel();
		nextBtn = new JButton("next");

		// backボタン
		backBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				// 位置を戻す
				next(puyofuList, canvas, master, -1);
			}});
		backBtn.setPreferredSize(new Dimension(80, 20));
		canvas.add(backBtn);

		// 位置
		posLabel.setPreferredSize(new Dimension(60, 20));
		canvas.add(posLabel);
		
		// nextボタン
		nextBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				// 位置を進める
				next(puyofuList, canvas, master, 1);
			}});
		nextBtn.setPreferredSize(new Dimension(80, 20));
		canvas.add(nextBtn);
		
		// フレーム作成
		new PuyoPuyoFrame("ぷよ譜").start(canvas);

		next(puyofuList, canvas, master, 1);
		canvas.drawPain(master);
	}

	private static void next(ArrayList<String> puyofuList, PuyoPain canvas, PuyoPuyoMaster master, int dir) {
		// 位置を進める
		if (pos + dir < 0 || pos + dir >= puyofuList.size()) {
			return;
		}
		pos += dir;

		// 位置ラベル更新
		posLabel.setText(String.format("%4d/%4d", pos, puyofuList.size()));

		// ぷよ譜読み込み
		String json = puyofuList.get(pos);
		Box box = gson.fromJson(json, Box.class);
		Puyo[][] arr = new Puyo[Box.ROW][Box.RANK];
		box.createPuyoArray(arr);

		int index;
		for (index = 0; index < 2; index++) {
			if (name[index] == null) {
				name[index] = new String(box.getName());
				break;
			} else if (name[index].equals(box.getName())) {
				break;
			}
		}

		// chainCountが0のEFFECTは読み飛ばし
		if (box.getState() == BoxState.EFFECT && box.getEffectCount() == 0) {
			next(puyofuList, canvas, master, dir);
			return;
		}
		
		// 現在のぷよを表示
		if (box.getEffectCount() == 0) {
			PuyoState currentPuyo = box.getCurrentPuyo();
			int currentRow = currentPuyo.getRow();
			int currentRank = currentPuyo.getRank(); 
			arr[currentRow][currentRank] = currentPuyo.getCluster().getFirst();
			switch (currentPuyo.getRotate()) {
			case R0:
				currentRow++;
				break;
			case R90:
				currentRank--;
				break;
			case R180:
				currentRow--;
				break;
			case R270:
				currentRank++;
				break;
			}
			arr[currentRow][currentRank] = currentPuyo.getCluster().getSecond();
		}
		
		// masterに設定
		Game game = master.getGame();
		if (index == 0) {
			game.setBox1(box);
		} else if (index == 1) {
			game.setBox2(box);
		}

		// puyoArray設定
		PuyoEx[][] puyoArray = null;
		if (index == 0) {
			puyoArray = master.getPuyoArray1();
		} else if (index == 1) {
			puyoArray = master.getPuyoArray2();
		}
		for (int i = 0; i < Box.ROW; i++) {
			for (int j = 0; j < Box.RANK; j++) {
				puyoArray[i][j].puyo = arr[i][j];
			}
		}

		// 描画
		canvas.drawPain(master);
	}
}