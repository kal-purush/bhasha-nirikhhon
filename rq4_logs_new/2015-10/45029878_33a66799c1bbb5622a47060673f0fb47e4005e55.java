import java.awt.*;
import javax.swing.*;
import java.util.Scanner;
import java.util.Arrays;
import javax.swing.GroupLayout.Alignment;
import net.miginfocom.swing.MigLayout;
import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.RowSpec;
import com.jgoodies.forms.factories.FormFactory;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.border.BevelBorder;
import javax.swing.border.MatteBorder;

public class lbc {

	public static int[][] errorPatternContainer = new int[][] {
			{ 0, 0, 0, 0, 0, 0, 0 }, { 0, 0, 0, 0, 0, 0, 1 },
			{ 0, 0, 0, 0, 0, 1, 0 }, { 0, 0, 0, 0, 1, 0, 0 },
			{ 0, 0, 0, 1, 0, 0, 0 }, { 0, 0, 1, 0, 0, 0, 0 },
			{ 0, 1, 0, 0, 0, 0, 0 }, { 1, 0, 0, 0, 0, 0, 0 } };
	private static JTextField textFieldM;
	// private static JTextField txtNull;
	private static boolean enCode = false;
	private static boolean setGen = false;

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		JFrame demo = new JFrame();
		demo.setSize(500, 874);
		demo.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		demo.setTitle("7-Bit Linear Block Coder | By Desmond Pang");
		demo.getContentPane().setLayout(null);

		JRadioButton rdbtnNewRadioButton = new JRadioButton("(7,3)");

		rdbtnNewRadioButton.setBounds(115, 6, 66, 37);
		demo.getContentPane().add(rdbtnNewRadioButton);

		JRadioButton rdbtnNewRadioButton_1 = new JRadioButton("(7,4)");
		rdbtnNewRadioButton_1.setSelected(true);
		rdbtnNewRadioButton_1.setBounds(193, 6, 66, 37);
		demo.getContentPane().add(rdbtnNewRadioButton_1);

		JRadioButton rdbtnNewRadioButton_2 = new JRadioButton("(7,5)");
		rdbtnNewRadioButton_2.setBounds(270, 6, 66, 37);
		demo.getContentPane().add(rdbtnNewRadioButton_2);

		ButtonGroup group = new ButtonGroup();
		group.add(rdbtnNewRadioButton);
		group.add(rdbtnNewRadioButton_1);
		group.add(rdbtnNewRadioButton_2);

		JTextArea txtrPleaseInputThe = new JTextArea();
		txtrPleaseInputThe.setBounds(6, 67, 488, 42);
		demo.getContentPane().add(txtrPleaseInputThe);

		textFieldM = new JTextField();
		textFieldM.setBounds(231, 139, 151, 37);
		textFieldM.setText(null);
		demo.getContentPane().add(textFieldM);

		// textFieldM.setColumns(10);

		JLabel lblMessage = new JLabel("Message  (Use space to separate)");
		lblMessage.setBounds(234, 121, 260, 16);
		demo.getContentPane().add(lblMessage);

		JLabel lblNewLabel = new JLabel(
				"Generator (Please input the generator matrix in single line with space.)");
		lblNewLabel.setBounds(6, 45, 488, 16);
		demo.getContentPane().add(lblNewLabel);

		JLabel lblCustomReceieveBits = new JLabel("Custom Received Bits");
		lblCustomReceieveBits.setBounds(11, 578, 161, 16);
		demo.getContentPane().add(lblCustomReceieveBits);

		JRadioButton radioButton = new JRadioButton("1");
		radioButton.setHorizontalAlignment(SwingConstants.CENTER);
		radioButton.setBounds(29, 606, 38, 37);
		radioButton.setVerticalTextPosition(SwingConstants.TOP);
		radioButton.setHorizontalTextPosition(SwingConstants.CENTER);

		demo.getContentPane().add(radioButton);

		JRadioButton radioButton_1 = new JRadioButton("1");
		radioButton_1.setVerticalTextPosition(SwingConstants.TOP);
		radioButton_1.setHorizontalTextPosition(SwingConstants.CENTER);
		radioButton_1.setHorizontalAlignment(SwingConstants.CENTER);
		radioButton_1.setBounds(96, 606, 38, 37);
		demo.getContentPane().add(radioButton_1);

		JRadioButton radioButton_2 = new JRadioButton("1");
		radioButton_2.setVerticalTextPosition(SwingConstants.TOP);
		radioButton_2.setHorizontalTextPosition(SwingConstants.CENTER);
		radioButton_2.setHorizontalAlignment(SwingConstants.CENTER);
		radioButton_2.setBounds(163, 606, 38, 37);
		demo.getContentPane().add(radioButton_2);

		JRadioButton radioButton_3 = new JRadioButton("1");
		radioButton_3.setVerticalTextPosition(SwingConstants.TOP);
		radioButton_3.setHorizontalTextPosition(SwingConstants.CENTER);
		radioButton_3.setHorizontalAlignment(SwingConstants.CENTER);
		radioButton_3.setBounds(230, 606, 38, 37);
		demo.getContentPane().add(radioButton_3);

		JRadioButton radioButton_4 = new JRadioButton("1");
		radioButton_4.setVerticalTextPosition(SwingConstants.TOP);
		radioButton_4.setHorizontalTextPosition(SwingConstants.CENTER);
		radioButton_4.setHorizontalAlignment(SwingConstants.CENTER);
		radioButton_4.setBounds(297, 606, 38, 37);
		demo.getContentPane().add(radioButton_4);

		JRadioButton radioButton_5 = new JRadioButton("1");
		radioButton_5.setVerticalTextPosition(SwingConstants.TOP);
		radioButton_5.setHorizontalTextPosition(SwingConstants.CENTER);
		radioButton_5.setHorizontalAlignment(SwingConstants.CENTER);
		radioButton_5.setBounds(364, 606, 38, 37);
		demo.getContentPane().add(radioButton_5);

		JRadioButton radioButton_6 = new JRadioButton("1");
		radioButton_6.setVerticalTextPosition(SwingConstants.TOP);
		radioButton_6.setHorizontalTextPosition(SwingConstants.CENTER);
		radioButton_6.setHorizontalAlignment(SwingConstants.CENTER);
		radioButton_6.setBounds(431, 606, 38, 37);
		demo.getContentPane().add(radioButton_6);

		JLabel lblNewLabel_1 = new JLabel("Minimun Distance:");
		lblNewLabel_1.setBounds(6, 126, 131, 16);
		demo.getContentPane().add(lblNewLabel_1);

		JLabel lblErrorDetectingCapability = new JLabel(
				"Error Detecting Capability:");
		lblErrorDetectingCapability.setBounds(6, 144, 175, 16);
		demo.getContentPane().add(lblErrorDetectingCapability);

		JLabel lblErrorCorrectingCapability = new JLabel(
				"Error Correcting Capability:");
		lblErrorCorrectingCapability.setBounds(6, 161, 175, 16);
		demo.getContentPane().add(lblErrorCorrectingCapability);

		JLabel dminT = new JLabel("0");
		dminT.setBounds(198, 126, 61, 16);
		demo.getContentPane().add(dminT);

		JLabel edcT = new JLabel("0");
		edcT.setBounds(198, 144, 61, 16);
		demo.getContentPane().add(edcT);

		JLabel eccT = new JLabel("0");
		eccT.setBounds(198, 161, 61, 16);
		demo.getContentPane().add(eccT);

		JLabel lblCorrectedCodeword_1 = new JLabel("Corrected Codeword");
		lblCorrectedCodeword_1.setBounds(10, 661, 161, 16);
		demo.getContentPane().add(lblCorrectedCodeword_1);

		JLabel lblCorrectedCodeword = new JLabel(
				"-               -               -               -               -               -               -");
		lblCorrectedCodeword.setForeground(new Color(0, 128, 128));
		lblCorrectedCodeword.setHorizontalAlignment(SwingConstants.CENTER);
		lblCorrectedCodeword.setBounds(6, 740, 488, 16);
		demo.getContentPane().add(lblCorrectedCodeword);

		JButton btnEncode = new JButton("Encode");
		btnEncode.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (enCode == false)
					enCode = true;
				else
					enCode = false;
			}
		});
		btnEncode.setBounds(394, 140, 89, 37);
		demo.getContentPane().add(btnEncode);

		JLabel fromErrorEncode = new JLabel(
				"-               -               -               -               -               -               -");
		fromErrorEncode.setForeground(Color.RED);
		fromErrorEncode.setVerticalAlignment(SwingConstants.TOP);
		fromErrorEncode.setHorizontalAlignment(SwingConstants.CENTER);
		fromErrorEncode.setBounds(6, 689, 488, 39);
		demo.getContentPane().add(fromErrorEncode);

		JLabel lblTo = new JLabel("to");
		lblTo.setHorizontalAlignment(SwingConstants.CENTER);
		lblTo.setBounds(6, 717, 488, 16);
		demo.getContentPane().add(lblTo);

		JLabel lblbitLinearBlock = new JLabel(
				"7-bit Linear Block Coder | Ver.1.0");
		lblbitLinearBlock.setForeground(Color.GRAY);
		lblbitLinearBlock.setHorizontalAlignment(SwingConstants.CENTER);
		lblbitLinearBlock.setBounds(6, 771, 488, 16);
		demo.getContentPane().add(lblbitLinearBlock);

		JLabel lblNewLabel_2 = new JLabel("| - - - - |");
		lblNewLabel_2.setForeground(Color.BLUE);
		lblNewLabel_2.setBounds(30, 241, 80, 92);
		demo.getContentPane().add(lblNewLabel_2);

		JLabel genMatrix = new JLabel(
				"<html>| - - - - - - - |<br>| - - - - - - - |<br>| - - - - - - - |<br>| - - - - - - - |<br><br>| - - - - - - - |</html>");
		genMatrix.setForeground(Color.BLUE);
		genMatrix.setBounds(112, 242, 98, 105);
		demo.getContentPane().add(genMatrix);

		JLabel lblEncodeProcess = new JLabel("Encode Process");
		lblEncodeProcess.setHorizontalAlignment(SwingConstants.CENTER);
		lblEncodeProcess.setBounds(40, 197, 161, 16);
		demo.getContentPane().add(lblEncodeProcess);

		JLabel lblDecodeProcess = new JLabel("Decode Process");
		lblDecodeProcess.setHorizontalAlignment(SwingConstants.CENTER);
		lblDecodeProcess.setBounds(289, 197, 161, 16);
		demo.getContentPane().add(lblDecodeProcess);

		JLabel recMessage = new JLabel("| - - - - - - |");
		recMessage.setForeground(Color.BLUE);
		recMessage.setHorizontalAlignment(SwingConstants.CENTER);
		recMessage.setBounds(258, 241, 109, 92);
		demo.getContentPane().add(recMessage);

		JLabel ht = new JLabel(
				"<html>| - - - - - - - |<br>| - - - - - - - |<br>| - - - - - - - |<br>|"
						+ " - - - - - - - |<br>| - - - - - - - |<br>| - - - - - - - |<br>| - - - - - - - |<br><br>| - - - - - - - |</html>");
		ht.setForeground(Color.BLUE);
		ht.setHorizontalAlignment(SwingConstants.CENTER);
		ht.setBounds(364, 242, 119, 151);
		demo.getContentPane().add(ht);

		JLabel lblSyndrome = new JLabel("Syndrome: ");
		lblSyndrome.setHorizontalAlignment(SwingConstants.CENTER);
		lblSyndrome.setBounds(249, 377, 89, 16);
		demo.getContentPane().add(lblSyndrome);

		JLabel lblErrorPattern = new JLabel("Error Pattern: ");
		lblErrorPattern.setHorizontalAlignment(SwingConstants.CENTER);
		lblErrorPattern.setBounds(257, 408, 89, 16);
		demo.getContentPane().add(lblErrorPattern);

		JLabel errorPattern = new JLabel("| - - - - - - |");
		errorPattern.setForeground(Color.RED);
		errorPattern.setHorizontalAlignment(SwingConstants.CENTER);
		errorPattern.setBounds(342, 405, 141, 23);
		demo.getContentPane().add(errorPattern);

		JLabel lblMessage_1 = new JLabel("Message");
		lblMessage_1.setHorizontalAlignment(SwingConstants.CENTER);
		lblMessage_1.setBounds(17, 219, 89, 16);
		demo.getContentPane().add(lblMessage_1);

		JLabel lblGenerator = new JLabel("Generator");
		lblGenerator.setHorizontalAlignment(SwingConstants.CENTER);
		lblGenerator.setBounds(118, 219, 89, 16);
		demo.getContentPane().add(lblGenerator);

		JLabel lblReceived = new JLabel("Received Message");
		lblReceived.setHorizontalAlignment(SwingConstants.CENTER);
		lblReceived.setBounds(238, 219, 131, 16);
		demo.getContentPane().add(lblReceived);

		JLabel lblTransposeOfHx = new JLabel("Transpose of H(x)");
		lblTransposeOfHx.setHorizontalAlignment(SwingConstants.CENTER);
		lblTransposeOfHx.setBounds(369, 219, 119, 16);
		demo.getContentPane().add(lblTransposeOfHx);

		JLabel lblErrorPatternSyndrome = new JLabel("Error Pattern  Syndrome");
		lblErrorPatternSyndrome.setHorizontalAlignment(SwingConstants.CENTER);
		lblErrorPatternSyndrome.setBounds(40, 374, 161, 23);
		demo.getContentPane().add(lblErrorPatternSyndrome);

		JLabel errorPatternSynTable = new JLabel("(Empty)");
		errorPatternSynTable.setForeground(Color.RED);
		errorPatternSynTable.setBackground(Color.LIGHT_GRAY);
		errorPatternSynTable.setHorizontalAlignment(SwingConstants.CENTER);
		errorPatternSynTable.setBounds(20, 408, 190, 151);
		demo.getContentPane().add(errorPatternSynTable);

		JLabel lblEncodedMessage = new JLabel("Encoded Message");
		lblEncodedMessage.setFont(new Font("Lucida Grande", Font.PLAIN, 11));
		lblEncodedMessage.setHorizontalAlignment(SwingConstants.CENTER);
		lblEncodedMessage.setBounds(9, 326, 99, 16);
		demo.getContentPane().add(lblEncodedMessage);

		JLayeredPane layeredPane = new JLayeredPane();
		layeredPane.setBorder(new MatteBorder(1, 1, 1, 1, (Color) new Color(
				128, 128, 128)));
		layeredPane.setBounds(6, 189, 220, 173);
		demo.getContentPane().add(layeredPane);

		JLayeredPane layeredPane_1 = new JLayeredPane();
		layeredPane_1.setBorder(new MatteBorder(1, 1, 1, 1, (Color) new Color(
				128, 128, 128)));
		layeredPane_1.setBounds(232, 188, 262, 245);
		demo.getContentPane().add(layeredPane_1);

		JLayeredPane layeredPane_2 = new JLayeredPane();
		layeredPane_2.setBorder(new MatteBorder(1, 1, 1, 1, (Color) new Color(
				128, 128, 128)));
		layeredPane_2.setForeground(new Color(0, 0, 0));
		layeredPane_2.setBounds(6, 370, 220, 194);
		demo.getContentPane().add(layeredPane_2);

		JLayeredPane layeredPane_3 = new JLayeredPane();
		layeredPane_3.setBorder(new MatteBorder(1, 1, 1, 1, (Color) new Color(
				128, 128, 128)));
		layeredPane_3.setBounds(6, 572, 488, 193);
		demo.getContentPane().add(layeredPane_3);

		JLabel lblAuthorDesmondPang = new JLabel(
				"Author: Desmond Pang | Email: bingbangdesmond@gmail.com");
		lblAuthorDesmondPang.setFont(new Font("Lucida Grande", Font.PLAIN, 10));
		lblAuthorDesmondPang.setHorizontalAlignment(SwingConstants.CENTER);
		lblAuthorDesmondPang.setForeground(Color.GRAY);
		lblAuthorDesmondPang.setBounds(6, 788, 488, 16);
		demo.getContentPane().add(lblAuthorDesmondPang);
		demo.setVisible(true);

		int messageLength;
		int countofRevisedBit = 0;
		int genLength = 0;

		String matrix_messge = "";
		String genMatrixString = "";
		String hTMatrixString = "";
		String errorPatternSynTableString = "";
		int fixedMessageLenghtis3 = 0;

		while (true) {
			messageLength = 0;

			if (rdbtnNewRadioButton.isSelected()) {
				messageLength = 3;
			} else if (rdbtnNewRadioButton_1.isSelected()) {
				messageLength = 4;

			} else if (rdbtnNewRadioButton_2.isSelected()) {
				messageLength = 5;
			}

			genLength = txtrPleaseInputThe.getText().length();
			// if (messageLength == 3 || messageLength == 4 || messageLength ==
			// 5) {

			System.out.println("");

			if (genLength >= (14 * messageLength - 1)) {

				rdbtnNewRadioButton.setEnabled(false);
				rdbtnNewRadioButton_1.setEnabled(false);
				rdbtnNewRadioButton_2.setEnabled(false);
				Scanner scan = new Scanner(txtrPleaseInputThe.getText());

				int generator[][] = new int[messageLength][7];
				for (int i = 0; i < generator.length; i++) {
					for (int j = 0; j < generator[0].length; j++) {

						int gElements = scan.nextInt();
						generator[i][j] = gElements;

						// System.out.print(i);
						// System.out.print(j);
						// System.out.print(generator[i][j]);
						// System.out.print(" ");
					}
					// System.out.println("");
				}

				int dmin = 0;
				int numberofONE = 0;

				for (int i = 0; i < messageLength; i++) {
					numberofONE = 0;
					for (int j = 0; j < (7 - messageLength); j++) {
						if (generator[i][j] == 1)
							numberofONE++;
						// System.out.println("numberofONE: "+numberofONE);
					}
					if (dmin < numberofONE) {
						dmin = numberofONE;
					}
					// System.out.println("DM: "+dmin);
				}
				// System.out.println("Minimun Distance is: "+dmin);
				// System.out.println("Error Detecting Capability is: "+
				// (dmin-1));
				// System.out.println("Error Correcting Capability is: "+
				// (dmin-1)/2);

				dminT.setText(dmin + "");
				edcT.setText((dmin - 1) + "");
				eccT.setText((dmin - 1) / 2 + "");

				if (enCode) {

					int messge[] = new int[messageLength];
					;
					String m = textFieldM.getText();
					int message;
					Scanner scanMessage = new Scanner(m);

					for (int j = 0; j < messge.length; j++) {

						// int message = scan.nextInt();
						message = scanMessage.nextInt();
						messge[j] = message;
					}

					System.out.println("Your Input Message is: "
							+ Arrays.toString(messge));

					// Generate H

					int h[][] = new int[7 - messageLength][7];
					for (int i = 0; i < (7 - messageLength); i++) {

						for (int j = 0; j < 7; j++) {
							if (j < (7 - messageLength)) {
								if (i == j)
									h[i][j] = 1;
								else
									h[i][j] = 0;
							} else {
								System.out.print(i + "" + j);
								h[i][j] = generator[j - (7 - messageLength)][i];
							}
						}
					}

					System.out.println("");
					System.out.println("H(x) is: ");
					for (int i = 0; i < h.length; i++) {
						for (int j = 0; j < h[0].length; j++) {
							System.out.print(h[i][j]);
							System.out.print(" ");
						}
						System.out.println("");
					}

					// Error Pattern

					if (messageLength == 3)
						fixedMessageLenghtis3 = 4;
					if (messageLength == 4)
						fixedMessageLenghtis3 = 4;
					if (messageLength == 5)
						fixedMessageLenghtis3 = 5;

					int syndrome[][] = new int[(int) Math.pow(2,
							7 - fixedMessageLenghtis3)][7 - messageLength];

					for (int i = 0; i < (int) Math.pow(2,
							7 - fixedMessageLenghtis3); i++) {
						for (int sCol = 0; sCol < (7 - messageLength); sCol++) {
							for (int pos = 0; pos < 7; pos++) {
								// System.out.println(pos);
								syndrome[i][sCol] = syndrome[i][sCol]
										+ (h[sCol][pos] * errorPatternContainer[i][pos]);
							}

							syndrome[i][sCol] = syndrome[i][sCol] % 2;
						}
					}

					errorPatternSynTableString = "<html>";
					for (int i = 0; i < (int) Math.pow(2,
							7 - fixedMessageLenghtis3); i++) {
						errorPatternSynTableString = errorPatternSynTableString
								+ Arrays.toString(errorPatternContainer[i])
								+ "  ";
						errorPatternSynTableString = errorPatternSynTableString
								+ Arrays.toString(syndrome[i]) + "<br>";
					}

					errorPatternSynTableString = errorPatternSynTableString
							+ "</html>";

					errorPatternSynTable.setText(errorPatternSynTableString);

					System.out.println("");
					System.out.println("Error Pattern e");
					for (int i = 0; i < syndrome.length; i++) {
						for (int j = 0; j < syndrome[0].length; j++) {
							System.out.print(syndrome[i][j]);
							System.out.print(" ");
						}
						System.out.println("");
					}

					// U = mG

					int codeword[] = new int[7];
					for (int col = 0; col < 7; col++) {
						for (int mulPos = 0; mulPos < messageLength; mulPos++) {

							codeword[col] = codeword[col]
									+ (messge[mulPos] * generator[mulPos][col]);
						}
						codeword[col] = codeword[col] % 2;
					}

					matrix_messge = "|";
					for (int i = 0; i < messageLength; i++) {
						matrix_messge = matrix_messge + messge[i] + " ";

					}
					matrix_messge = matrix_messge + "|";
					lblNewLabel_2.setText(matrix_messge);

					genMatrixString = "<html>";
					for (int i = 0; i < messageLength; i++) {
						genMatrixString = genMatrixString + "|";
						for (int j = 0; j < 7; j++) {
							genMatrixString = genMatrixString + generator[i][j]
									+ " ";
						}
						genMatrixString = genMatrixString + "|<br>";
					}

					genMatrixString = genMatrixString + "<br>|" + codeword[0]
							+ " " + codeword[1] + " " + codeword[2] + " "
							+ codeword[3] + " " + codeword[4] + " "
							+ codeword[5] + " " + codeword[6] + " |";
					genMatrixString = genMatrixString + "</html>";
					genMatrix.setText(genMatrixString);

					System.out.println("codeword is: "
							+ Arrays.toString(codeword));
					radioButton.setText("" + codeword[0]);
					radioButton_1.setText("" + codeword[1]);
					radioButton_2.setText("" + codeword[2]);
					radioButton_3.setText("" + codeword[3]);
					radioButton_4.setText("" + codeword[4]);
					radioButton_5.setText("" + codeword[5]);
					radioButton_6.setText("" + codeword[6]);

					if ((dmin - 1) / 2 >= 1) {
						// for (int i = 0; i < codeword.length; i++) {
						// int userConfirm = scan.nextInt();
						// if(userConfirm==0){
						// codeword[i]=(codeword[i]+1)%2;
						// System.out.println(codeword[i]+" ");
						// }else{
						// System.out.println(codeword[i]+" ");
						// }
						// }

						countofRevisedBit = 0;

						if (radioButton.isSelected()) {
							codeword[0] = (codeword[0] + 1) % 2;
							countofRevisedBit++;
						}
						if (radioButton_1.isSelected()) {
							codeword[1] = (codeword[1] + 1) % 2;
							countofRevisedBit++;
						}
						if (radioButton_2.isSelected()) {
							codeword[2] = (codeword[2] + 1) % 2;
							countofRevisedBit++;
						}
						if (radioButton_3.isSelected()) {
							codeword[3] = (codeword[3] + 1) % 2;
							countofRevisedBit++;
						}
						if (radioButton_4.isSelected()) {
							codeword[4] = (codeword[4] + 1) % 2;
							countofRevisedBit++;
						}
						if (radioButton_5.isSelected()) {
							codeword[5] = (codeword[5] + 1) % 2;
							countofRevisedBit++;
						}
						if (radioButton_6.isSelected()) {
							codeword[6] = (codeword[6] + 1) % 2;
							countofRevisedBit++;
						}

						// System.out.println("codeword is: "+
						// Arrays.toString(codeword));
						if (countofRevisedBit == 0)
							errorPattern.setText("No error");

						if (countofRevisedBit <= 1)
							fromErrorEncode.setText("" + codeword[0]
									+ "               " + codeword[1]
									+ "               " + codeword[2]
									+ "               " + codeword[3]
									+ "               " + codeword[4]
									+ "               " + codeword[5]
									+ "               " + codeword[6]);
						else
							fromErrorEncode
									.setText("<html> As the Error Correcting Capability is 1, it cannot correct more than 1 bit.</html>");

						recMessage.setText(" |" + codeword[0] + " "
								+ codeword[1] + " " + codeword[2] + " "
								+ codeword[3] + " " + codeword[4] + " "
								+ codeword[5] + " " + codeword[6] + " |");

						hTMatrixString = "<html>";
						for (int i = 0; i < 7; i++) {
							hTMatrixString = hTMatrixString + "|";
							for (int j = 0; j < (7 - messageLength); j++) {
								hTMatrixString = hTMatrixString + h[j][i] + " ";
							}
							hTMatrixString = hTMatrixString + "|<br>";
						}

						// Check receive Syndrome
						int recSyndrome[] = new int[7 - messageLength];

						// for (int i = 0; i < ((int) Math.pow(2,
						// 7-messageLength)); i++) {
						for (int sCol = 0; sCol < (7 - messageLength); sCol++) {
							for (int pos = 0; pos < 7; pos++) {

								recSyndrome[sCol] = recSyndrome[sCol]
										+ (h[sCol][pos] * codeword[pos]);
							}

							recSyndrome[sCol] = recSyndrome[sCol] % 2;
						}
						// }

						System.out.println("RecSyndrome is: "
								+ Arrays.toString(recSyndrome));

						hTMatrixString = hTMatrixString + "<br>"
								+ Arrays.toString(recSyndrome) + "</html>";
						ht.setText(hTMatrixString);

						for (int eachE = 0; eachE < ((int) Math.pow(2,
								7 - fixedMessageLenghtis3)); eachE++) {

							int tempA[] = new int[7 - messageLength];
							tempA = syndrome[eachE];
							System.out.println("tempA tempA is: "
									+ Arrays.toString(tempA));
							System.out
									.println("Error Error is: "
											+ Arrays.toString(errorPatternContainer[eachE]));

							if (Arrays.equals(tempA, recSyndrome)) {
								// Correction
								System.out.println("Error Pattern is: "
										+ Arrays.toString(syndrome[eachE]));
								System.out
										.println("Error Pattern is: "
												+ Arrays.toString(errorPatternContainer[eachE]));
								for (int a = 0; a < 7; a++) {
									if (errorPatternContainer[eachE][a] == 1) {
										errorPattern
												.setText(""
														+ Arrays.toString(errorPatternContainer[eachE]));
										codeword[a] = (codeword[a] + 1) % 2;

									}
								}
								// break;
							}
						}

						if (countofRevisedBit <= 1)
							lblCorrectedCodeword.setText("" + codeword[0]
									+ "               " + codeword[1]
									+ "               " + codeword[2]
									+ "               " + codeword[3]
									+ "               " + codeword[4]
									+ "               " + codeword[5]
									+ "               " + codeword[6]);
						else
							lblCorrectedCodeword
									.setText("-               -               -               -"
											+ "               -               -               -");
					} else {

						System.out
								.println("Since The Error Correcting Capability Is <1, you cannot correct the error of the received code");
						fromErrorEncode
								.setText("<html>As the Error Correcting Capability is smaller 1, it cannot correct any bit of the code.</html>");
					}

				}
				// System.out.println("Final Corrected codeword is: "+
				// Arrays.toString(codeword));

			}

			assert Boolean.TRUE;

		}

	}
}