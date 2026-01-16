/**
 * FINAL PROJECT
 * Class: CSCI2070 Ethics/Cyber Security
 * Instructor: Dr. Amar Rasheed

 * Documentation: Michael Johnson
 * Crypto Operations Code: Gabriel Tomberlin
 * JavaFX GUI: Jordan Holmes

 */

/** READ-ME FOR KEYS:
 * When inputting keys, they MUST be in XX format to work.
 * This means instead of 1, it must be 01.
 * An example key with no diagonal swaps for the sub-blocks: 010203040506070809
   
*/

import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.stage.Stage;

import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.scene.control.Label;
import javafx.scene.control.RadioButton;
import javafx.scene.control.ToggleGroup;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;

public class CipherProject extends Application implements EventHandler<ActionEvent> {

    static Button btnE = new Button("Encrypt");
    static Button SKbtn1 = new Button("Check SubKey");
    static Button LKbtn = new Button("Check LargeKey");
    static TextField plaintextField = new TextField();
    static TextField subkeyField = new TextField();
    static TextField largeKeyField = new TextField();
    static TextField encryptField = new TextField();
    static Label sKeyLabel = new Label(" In any order, you must list numbers"
            + " 1 - 9 (in XX format; ex: 010304...09)");
    static Label lKeyLabel = new Label(" In any order, you must list numbers"
            + " 1 - 19 (in XX format; ex: 010304...19)");
    static Label checkLabel1 = new Label();
    static Label checkLabel2 = new Label();
    static Label instruction = new Label(" ENTER 1-12 CHARACTERS (spaces included)");
    static Label spaceLabel1 = new Label(" ");
    static Label spaceLabel2 = new Label(" ");
    static Label spaceLabel3 = new Label(" ");

    String subkeyFieldString;
    String largeKeyFieldString;


    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) throws Exception {
        primaryStage.setTitle("Encryption");

        plaintextField.setPromptText("Enter your text to be encrypted");
        subkeyField.setPromptText("Enter your sub-blocks key");
        largeKeyField.setPromptText("Enter your large block key");
        encryptField.setPromptText("Here is the encryption");
        checkLabel1.setTextFill(Color.RED);
        checkLabel2.setTextFill(Color.RED);

        GridPane layout = new GridPane();

        layout.add(instruction, 0, 2, 12, 1);
        layout.add(plaintextField, 0, 3, 12, 1);
        layout.add(spaceLabel1, 0, 4, 12, 1);
        layout.add(sKeyLabel, 0, 5, 12, 1);
        layout.add(subkeyField, 0, 6, 7, 1);
        layout.add(SKbtn1, 8, 6, 1, 1);
        layout.add(checkLabel1, 9, 6, 5, 1);

        layout.add(spaceLabel2, 0, 7, 12, 1);
        layout.add(lKeyLabel, 0, 8, 12, 1);
        layout.add(largeKeyField, 0, 9, 7, 1);
        layout.add(LKbtn, 8, 9, 1, 1);
        layout.add(spaceLabel3, 0, 10, 12, 1);
        layout.add(btnE, 0, 11, 1, 1);
        layout.add(encryptField, 0, 12, 12, 1);
        layout.add(checkLabel2, 9, 9, 5, 1);

        encryptField.setEditable(false);


        VBox paneForRadioButtons = new VBox(20);
        paneForRadioButtons.setPadding(new Insets(5, 5, 5, 5));

        RadioButton rbECB = new RadioButton("ECB Mode");
        RadioButton rbCTR = new RadioButton("CTR Mode");
        RadioButton rbCBC = new RadioButton("CBC Mode");
        paneForRadioButtons.getChildren().addAll(rbECB, rbCTR, rbCBC);

        ToggleGroup group = new ToggleGroup();
        rbECB.setToggleGroup(group);
        rbCTR.setToggleGroup(group);
        rbCBC.setToggleGroup(group);


        layout.getChildren().addAll(paneForRadioButtons);
        Scene scene = new Scene(layout, 450, 350);
        primaryStage.setScene(scene);
        primaryStage.show();

        // WORKS!! Check sub key!
        SKbtn1.setOnAction(e -> {
            try {
                subkeyFieldString = subkeyField.getText();
                BlockEncryption.checkSubKey(subkeyFieldString);
            } catch (Exception ex) {
                Logger.getLogger(CipherProject.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
        
        LKbtn.setOnAction(e -> {
            try {
                largeKeyFieldString = largeKeyField.getText();
                BlockEncryption.checkLargeKey(largeKeyFieldString);
            } catch (Exception ex) {
                Logger.getLogger(CipherProject.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
        
        btnE.setOnAction(e -> {
            try {
                String plaintext = plaintextField.getText();
                subkeyFieldString = subkeyField.getText();
                largeKeyFieldString = largeKeyField.getText();
                
                if (rbECB.isSelected()) {
                    BlockEncryption.encrypt(plaintext, 1, 0, 0, subkeyFieldString,
                            largeKeyFieldString);
                } else if (rbCTR.isSelected()) {
                    BlockEncryption.encrypt(plaintext, 0, 1, 0, subkeyFieldString,
                            largeKeyFieldString);
                } else if (rbCBC.isSelected()) {
                    BlockEncryption.encrypt(plaintext, 0, 0, 1, subkeyFieldString,
                            largeKeyFieldString);
                }
                
                
            } catch (Exception ex) {
                Logger.getLogger(CipherProject.class.getName()).log(Level.SEVERE, null, ex);
            }
        });


    }

    @Override
    public void handle(ActionEvent event) {
        if (event.getSource() == SKbtn1) {
            System.out.println("Key 1 logged");
        }
        if (event.getSource() == LKbtn) {
            System.out.println("Key 5 logged");
        }
        if (event.getSource() == btnE) {
            System.out.println("You just got encrypted!!");
        }
    }
}

class BlockEncryption {
    
    static char[][] largeBlock = new char[10][10];
    static char[][] subBlock1 = new char[5][5];
    static char[][] subBlock2 = new char[5][5];
    static char[][] subBlock3 = new char[5][5];
    static char[][] subBlock4 = new char[5][5];
        
        // Split the largeBlock into 4 subBlocks -- DONE (tested by printing each array)
        // Swap diagonals in each subBlock -- DONE
            // key verification method -- DONE
        // Put subBlocks back together -- DONE
        // Swap diagonals of newly created largeBlock -- DONE
        // Output the ciphertext into text field -- DONE
         
    public static void encrypt(String plaintext, int ECB, int CTR, int CBC, String subKey, String largeKey) {

        CryptoModes ECBEncrypt = new CryptoModes();
        CryptoModes CTREncrypt = new CryptoModes();
        CryptoModes CBCEncrypt = new CryptoModes();

        if (ECB == 1) {
            ECBEncrypt.ECB(plaintext, largeBlock, subBlock1, subBlock2,
                    subBlock3, subBlock4, subKey, largeKey);
        } else if (CTR == 1) {
            CTREncrypt.CTR(plaintext, largeBlock, subBlock1, subBlock2,
                    subBlock3, subBlock4, subKey, largeKey);
        } else if (CBC == 1) {
            CBCEncrypt.CBC(plaintext, largeBlock, subBlock1, subBlock2,
                    subBlock3, subBlock4, subKey, largeKey);
        }
        
    }
    
    public static void checkSubKey(String subKey) throws Exception {
        
        if (!"01".equals(subKey.substring(0, 2)) && !"09".equals(subKey.substring(0, 2))) {
            CipherProject.checkLabel1.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "You can only swap diagonals with equal number of entries!");
            
        } else if (!"02".equals(subKey.substring(2, 4)) && !"08".equals(subKey.substring(2, 4))) {
            CipherProject.checkLabel1.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "You can only swap diagonals with equal number of entries!");
            
        } else if (!"03".equals(subKey.substring(4, 6)) && !"07".equals(subKey.substring(4, 6))) {
            CipherProject.checkLabel1.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "You can only swap diagonals with equal number of entries!");
            
        } else if (!"04".equals(subKey.substring(6, 8)) && !"06".equals(subKey.substring(6, 8))) {
            CipherProject.checkLabel1.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "You can only swap diagonals with equal number of entries!");
            
        } else if (!"05".equals(subKey.substring(8, 10))) {
            CipherProject.checkLabel1.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "The middle diagonal cannot be swapped with any other diagonal!");
        } else {
            CipherProject.checkLabel1.setText("Key is good!");
        }
        
    }
    
    public static void checkLargeKey(String largeKey) throws Exception {
        
        if ((!"01".equals(largeKey.substring(0, 2)) && !"19".equals(largeKey.substring(0, 2)))
                || (!"01".equals(largeKey.substring(36, 38)) && 
                !"19".equals(largeKey.substring(36, 38)))) {
            CipherProject.checkLabel2.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "You can only swap diagonals with equal number of entries!");
            
        } else if (!"02".equals(largeKey.substring(2, 4)) && !"18".equals(largeKey.substring(2, 4))
                || (!"02".equals(largeKey.substring(34, 36)) && 
                !"18".equals(largeKey.substring(34, 36)))) {
            CipherProject.checkLabel2.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "You can only swap diagonals with equal number of entries!");
            
        } else if (!"03".equals(largeKey.substring(4, 6)) && !"17".equals(largeKey.substring(4, 6))
                || (!"03".equals(largeKey.substring(32, 34)) && 
                !"17".equals(largeKey.substring(32, 34)))) {
            CipherProject.checkLabel2.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "You can only swap diagonals with equal number of entries!");
            
        } else if (!"04".equals(largeKey.substring(6, 8)) && !"16".equals(largeKey.substring(6, 8))
                || (!"04".equals(largeKey.substring(30, 32)) && 
                !"16".equals(largeKey.substring(30, 32)))) {
            CipherProject.checkLabel2.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "You can only swap diagonals with equal number of entries!");
            
        } else if (!"05".equals(largeKey.substring(8, 10)) && !"15".equals(largeKey.substring(8, 10))
                || (!"05".equals(largeKey.substring(28, 30)) && 
                !"15".equals(largeKey.substring(28, 30)))) {
            CipherProject.checkLabel2.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "You can only swap diagonals with equal number of entries!");
            
        } else if (!"06".equals(largeKey.substring(10, 12)) && !"14".equals(largeKey.substring(10, 12))
                || (!"06".equals(largeKey.substring(26, 28)) && 
                !"14".equals(largeKey.substring(26, 28)))) {
            CipherProject.checkLabel2.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "You can only swap diagonals with equal number of entries!");
            
        } else if (!"07".equals(largeKey.substring(12, 14)) && !"13".equals(largeKey.substring(12, 14))
                || (!"07".equals(largeKey.substring(24, 26)) && 
                !"13".equals(largeKey.substring(24, 26)))) {
            CipherProject.checkLabel2.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "You can only swap diagonals with equal number of entries!");
            
        } else if (!"08".equals(largeKey.substring(14, 16)) && !"12".equals(largeKey.substring(14, 16))
                || (!"08".equals(largeKey.substring(22, 24)) && 
                !"12".equals(largeKey.substring(22, 24)))) {
            CipherProject.checkLabel2.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "You can only swap diagonals with equal number of entries!");
            
        } else if (!"09".equals(largeKey.substring(16, 18)) && !"11".equals(largeKey.substring(16, 18))
                || (!"09".equals(largeKey.substring(20, 22)) && 
                !"11".equals(largeKey.substring(20, 22)))) {
            CipherProject.checkLabel2.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "You can only swap diagonals with equal number of entries!");
            
        } else if (!"10".equals(largeKey.substring(18, 20))) {
            CipherProject.checkLabel2.setText("Incorrect key format!");
            throw new Exception ("Illegal format of key. "
                    + "The middle diagonal cannot be swapped with any other diagonal!");
        } else {
            CipherProject.checkLabel2.setText("Key is good!");
        }
        
    }
    
    
private static class CryptoModes {
    
    char temp1 = 0;
    char temp2 = 0;
    char temp3 = 0;
    char temp4 = 0;
    char temp5 = 0;
    char temp6 = 0;
    char temp7 = 0;
    char temp8 = 0;
    char temp9 = 0;

    private final StringBuilder sbIV = new StringBuilder("10101001100001110110101111"
            + "001111111001000101001100101"
            + "0011000101011011011110011111111001000110111");
    
    private void ECB(String binaryString, char[][] largeBlock, char[][] subBlock1, 
                char[][] subBlock2, char[][] subBlock3, char[][] subBlock4, String
                        subKey, String largeKey) {
        
        final String encryptKey = "111101010100101010000010010110011011000100111"
            + "001001011001001001101101001101110101010011010100101";
        
            StringBuilder BSappend = new StringBuilder(binaryString);
            while (BSappend.length() < 12) {
                BSappend.append(" ");
            }
            
            binaryString = BSappend.toString();
            
        byte[] bytes = binaryString.getBytes();
        StringBuilder binary = new StringBuilder();
        for (byte b : bytes) {
            int val = b;
            for (int i = 0; i < 8; i++) {
                binary.append((val & 128) == 0 ? 0 : 1);
                val <<= 1;
            }
        }

        // Convert the StringBuilder to a String
        String appendedPlaintext = binary.toString();

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < appendedPlaintext.length(); i++) {
            sb.append((appendedPlaintext.charAt(i) ^ encryptKey.charAt(i)));
        }

        // Pad the binary string with 4 space characters at the end
        sb.append(" " + " " + " " + " ");

        String cipherText = sb.toString();

        // Populate the array
        int k = 0;
        for (int i = 0; i < largeBlock.length; i++) {
            for (int j = 0; j < largeBlock[i].length; j++) {
                largeBlock[i][j] = cipherText.charAt(k);
                k++;
            }
        }

        /*
                        0  1  1  0  0 | 0  0  1  0  1  
                        1  0  0  0  1 | 0  0  1  1  0  
       subBlock1 -->    0  0  1  1  0 | 1  1  0  0  1  <-- subBlock2
                        0  0  0  1  1 | 0  0  1  0  1  
                        0  1  1  0  0 | 1  1  0  0  1  
                        -----------------------------
                        1  0  0  1  1 | 1  0  1  1  0  
                        1  0  0  0  0 | 1  1  0  1  0  
       subBlock3 -->    0  1  0  1  1 | 0  1  0  1  0  <-- subBlock4
                        0  1  1  0  1 | 0  1  1  0  1  
                        1  0  1  1  0 | 0              
        */
        

            // Populate subBlock1
            for (int i = 0; i < subBlock1.length; i++) {
                for (int j = 0; j < subBlock1[i].length; j++) {
                    subBlock1[i][j] = largeBlock[i][j];

                }
            }

            // Populate subBlock2
            for (int i = 0; i < subBlock2.length; i++) {
                for (int j = 0; j < subBlock2[i].length; j++) {
                    subBlock2[i][j] = largeBlock[i][j + 5];

                }
            }

            // Populate subBlock3
            for (int i = 0; i < subBlock3.length; i++) {
                for (int j = 0; j < subBlock3[i].length; j++) {
                    subBlock3[i][j] = largeBlock[i + 5][j];

                }
            }

            // Populate subBlock4
            for (int i = 0; i < subBlock4.length; i++) {
                for (int j = 0; j < subBlock4[i].length; j++) {
                    subBlock4[i][j] = largeBlock[i + 5][j + 5];

                }
            }
            
            // Diagonal swap the subBlocks
            if ("09".equals(subKey.substring(0, 2))) {
                temp1 = subBlock1[0][0];
                subBlock1[0][0] = subBlock1[4][4];
                subBlock1[4][4] = temp1;
                
                temp1 = subBlock2[0][0];
                subBlock1[0][0] = subBlock1[4][4];
                subBlock1[4][4] = temp1;

                temp1 = subBlock3[0][0];
                subBlock1[0][0] = subBlock1[4][4];
                subBlock1[4][4] = temp1;

                temp1 = subBlock4[0][0];
                subBlock1[0][0] = subBlock1[4][4];
                subBlock1[4][4] = temp1;
                
                
            } else if ("08".equals(subKey.substring(2, 4))) {
                temp1 = subBlock1[0][1];
                temp2 = subBlock1[1][0];
                subBlock1[1][0] = subBlock1[4][3];
                subBlock1[0][1] = subBlock1[3][4];
                subBlock1[4][3] = temp1;
                subBlock1[3][4] = temp2;

                temp1 = subBlock2[0][1];
                temp2 = subBlock2[1][0];
                subBlock2[0][1] = subBlock2[4][3];
                subBlock2[1][0] = subBlock2[3][4];
                subBlock2[4][3] = temp1;
                subBlock2[3][4] = temp2;

                temp1 = subBlock3[0][1];
                temp2 = subBlock3[1][0];
                subBlock3[0][1] = subBlock3[4][3];
                subBlock3[1][0] = subBlock3[3][4];
                subBlock3[4][3] = temp1;
                subBlock3[3][4] = temp2;

                temp1 = subBlock4[0][1];
                temp2 = subBlock4[1][0];
                subBlock4[0][1] = subBlock4[4][3];
                subBlock4[1][0] = subBlock4[3][4];
                subBlock4[4][3] = temp1;
                subBlock4[3][4] = temp2;
                
            } else if ("07".equals(subKey.substring(4, 6))) {
                temp1 = subBlock1[2][0];
                temp2 = subBlock1[1][1];
                temp3 = subBlock1[0][2];
                subBlock1[2][0] = subBlock1[4][2];
                subBlock1[1][1] = subBlock1[3][3];
                subBlock1[0][2] = subBlock1[2][4];
                subBlock1[4][2] = temp1;
                subBlock1[3][3] = temp2;
                subBlock1[2][4] = temp3;
                
                temp1 = subBlock2[2][0];
                temp2 = subBlock2[1][1];
                temp3 = subBlock2[0][2];
                subBlock2[2][0] = subBlock2[4][2];
                subBlock2[1][1] = subBlock2[3][3];
                subBlock2[0][2] = subBlock2[2][4];
                subBlock2[4][2] = temp1;
                subBlock2[3][3] = temp2;
                subBlock2[2][4] = temp3;

                temp1 = subBlock3[2][0];
                temp2 = subBlock3[1][1];
                temp3 = subBlock3[0][2];
                subBlock3[2][0] = subBlock3[4][2];
                subBlock3[1][1] = subBlock3[3][3];
                subBlock3[0][2] = subBlock3[2][4];
                subBlock3[4][2] = temp1;
                subBlock3[3][3] = temp2;
                subBlock3[2][4] = temp3;

                temp1 = subBlock4[2][0];
                temp2 = subBlock4[1][1];
                temp3 = subBlock4[0][2];
                subBlock4[2][0] = subBlock4[4][2];
                subBlock4[1][1] = subBlock4[3][3];
                subBlock4[0][2] = subBlock4[2][4];
                subBlock4[4][2] = temp1;
                subBlock4[3][3] = temp2;
                subBlock4[2][4] = temp3;
                
            } else if ("06".equals(subKey.substring(6, 8))) {
                temp1 = subBlock1[3][0];
                temp2 = subBlock1[2][1];
                temp3 = subBlock1[1][2];
                temp4 = subBlock1[0][3];
                subBlock1[3][0] = subBlock1[4][1];
                subBlock1[2][1] = subBlock1[3][2];
                subBlock1[1][2] = subBlock1[2][3];
                subBlock1[0][3] = subBlock1[1][4];
                subBlock1[4][1] = temp1;
                subBlock1[3][2] = temp2;
                subBlock1[2][3] = temp3;
                subBlock1[1][4] = temp4;

                temp1 = subBlock2[3][0];
                temp2 = subBlock2[2][1];
                temp3 = subBlock2[1][2];
                temp4 = subBlock2[0][3];
                subBlock2[3][0] = subBlock2[4][1];
                subBlock2[2][1] = subBlock2[3][2];
                subBlock2[1][2] = subBlock2[2][3];
                subBlock2[0][3] = subBlock2[1][4];
                subBlock2[4][1] = temp1;
                subBlock2[3][2] = temp2;
                subBlock2[2][3] = temp3;
                subBlock2[1][4] = temp4;

                temp1 = subBlock3[3][0];
                temp2 = subBlock3[2][1];
                temp3 = subBlock3[1][2];
                temp4 = subBlock3[0][3];
                subBlock3[3][0] = subBlock3[4][1];
                subBlock3[2][1] = subBlock3[3][2];
                subBlock3[1][2] = subBlock3[2][3];
                subBlock3[0][3] = subBlock3[1][4];
                subBlock3[4][1] = temp1;
                subBlock3[3][2] = temp2;
                subBlock3[2][3] = temp3;
                subBlock3[1][4] = temp4;

                temp1 = subBlock4[3][0];
                temp2 = subBlock4[2][1];
                temp3 = subBlock4[1][2];
                temp4 = subBlock4[0][3];
                subBlock4[3][0] = subBlock4[4][1];
                subBlock4[2][1] = subBlock4[3][2];
                subBlock4[1][2] = subBlock4[2][3];
                subBlock4[0][3] = subBlock4[1][4];
                subBlock4[4][1] = temp1;
                subBlock4[3][2] = temp2;
                subBlock4[2][3] = temp3;
                subBlock4[1][4] = temp4;
            }
            
            // Diagonal swap the largeBlock
            if ("19".equals(largeKey.substring(0, 2))) {
                temp1 = largeBlock[0][0];
                largeBlock[0][0] = largeBlock[9][9];
                largeBlock[9][9] = temp1;
                
            } else if ("18".equals(largeKey.substring(2, 4))) {
                temp1 = largeBlock[1][0];
                temp2 = largeBlock[0][1];
                largeBlock[1][0] = largeBlock[9][8];
                largeBlock[0][1] = largeBlock[8][9];
                largeBlock[9][8] = temp1;
                largeBlock[8][9] = temp2;
                
            } else if ("17".equals(largeKey.substring(4, 6))) {
                temp1 = largeBlock[2][0];
                temp2 = largeBlock[1][1];
                temp3 = largeBlock[0][2];
                largeBlock[2][0] = largeBlock[9][7];
                largeBlock[1][1] = largeBlock[8][8];
                largeBlock[0][2] = largeBlock[7][9];
                largeBlock[9][7] = temp1;
                largeBlock[8][8] = temp2;
                largeBlock[7][9] = temp3;
                
            } else if ("16".equals(largeKey.substring(6, 8))) {
                temp1 = largeBlock[3][0];
                temp2 = largeBlock[2][1];
                temp3 = largeBlock[1][2];
                temp4 = largeBlock[0][3];
                largeBlock[3][0] = largeBlock[9][6];
                largeBlock[2][1] = largeBlock[8][7];
                largeBlock[1][2] = largeBlock[7][8];
                largeBlock[0][3] = largeBlock[6][9];
                largeBlock[9][6] = temp1;
                largeBlock[8][7] = temp2;
                largeBlock[7][8] = temp3;
                largeBlock[6][9] = temp4;
                
            } else if ("15".equals(largeKey.substring(8, 10))) {
                temp1 = largeBlock[4][0];
                temp2 = largeBlock[3][1];
                temp3 = largeBlock[2][2];
                temp4 = largeBlock[1][3];
                temp5 = largeBlock[0][4];
                largeBlock[4][0] = largeBlock[9][5];
                largeBlock[3][1] = largeBlock[8][6];
                largeBlock[2][2] = largeBlock[7][7];
                largeBlock[1][3] = largeBlock[6][8];
                largeBlock[0][4] = largeBlock[5][9];
                largeBlock[9][5] = temp1;
                largeBlock[8][6] = temp2;
                largeBlock[7][7] = temp3;
                largeBlock[6][8] = temp4;
                largeBlock[5][9] = temp5;
                
            } else if ("14".equals(largeKey.substring(10, 12))) {
                temp1 = largeBlock[5][0];
                temp2 = largeBlock[4][1];
                temp3 = largeBlock[3][2];
                temp4 = largeBlock[2][3];
                temp5 = largeBlock[1][4];
                temp6 = largeBlock[0][5];
                largeBlock[5][0] = largeBlock[9][4];
                largeBlock[4][1] = largeBlock[8][5];
                largeBlock[3][2] = largeBlock[7][6];
                largeBlock[2][3] = largeBlock[6][7];
                largeBlock[1][4] = largeBlock[5][8];
                largeBlock[0][5] = largeBlock[4][9];
                largeBlock[9][4] = temp1;
                largeBlock[8][5] = temp2;
                largeBlock[7][6] = temp3;
                largeBlock[6][7] = temp4;
                largeBlock[5][8] = temp5;
                largeBlock[4][9] = temp6;
                
            } else if ("13".equals(largeKey.substring(12, 14))) {
                temp1 = largeBlock[6][0];
                temp2 = largeBlock[5][1];
                temp3 = largeBlock[4][2];
                temp4 = largeBlock[3][3];
                temp5 = largeBlock[2][4];
                temp6 = largeBlock[1][5];
                temp7 = largeBlock[0][6];
                largeBlock[6][0] = largeBlock[9][3];
                largeBlock[5][1] = largeBlock[8][4];
                largeBlock[4][2] = largeBlock[7][5];
                largeBlock[3][3] = largeBlock[6][6];
                largeBlock[2][4] = largeBlock[5][7];
                largeBlock[1][5] = largeBlock[4][8];
                largeBlock[0][6] = largeBlock[3][9];
                largeBlock[9][3] = temp1;
                largeBlock[8][4] = temp2;
                largeBlock[7][5] = temp3;
                largeBlock[6][6] = temp4;
                largeBlock[5][7] = temp5;
                largeBlock[4][8] = temp6;
                largeBlock[3][9] = temp7;
                
            } else if ("12".equals(largeKey.substring(14, 16))) {
                temp1 = largeBlock[7][0];
                temp2 = largeBlock[6][1];
                temp3 = largeBlock[5][2];
                temp4 = largeBlock[4][3];
                temp5 = largeBlock[3][4];
                temp6 = largeBlock[2][5];
                temp7 = largeBlock[1][6];
                temp8 = largeBlock[0][7];
                largeBlock[7][0] = largeBlock[9][2];
                largeBlock[6][1] = largeBlock[8][3];
                largeBlock[5][2] = largeBlock[7][4];
                largeBlock[4][3] = largeBlock[6][5];
                largeBlock[3][4] = largeBlock[5][6];
                largeBlock[2][5] = largeBlock[4][7];
                largeBlock[1][6] = largeBlock[3][8];
                largeBlock[0][7] = largeBlock[2][9];
                largeBlock[9][2] = temp1;
                largeBlock[8][3] = temp2;
                largeBlock[7][4] = temp3;
                largeBlock[6][5] = temp4;
                largeBlock[5][6] = temp5;
                largeBlock[4][7] = temp6;
                largeBlock[3][8] = temp7;
                largeBlock[2][9] = temp8;
                
            } else if ("11".equals(largeKey.substring(16, 18))) {
                temp1 = largeBlock[8][0];
                temp2 = largeBlock[7][1];
                temp3 = largeBlock[6][2];
                temp4 = largeBlock[5][3];
                temp5 = largeBlock[4][4];
                temp6 = largeBlock[3][5];
                temp7 = largeBlock[2][6];
                temp8 = largeBlock[1][7];
                temp9 = largeBlock[0][8];
                largeBlock[8][0] = largeBlock[9][1];
                largeBlock[7][1] = largeBlock[8][2];
                largeBlock[6][2] = largeBlock[7][3];
                largeBlock[5][3] = largeBlock[6][4];
                largeBlock[4][4] = largeBlock[5][5];
                largeBlock[3][5] = largeBlock[4][6];
                largeBlock[2][6] = largeBlock[3][7];
                largeBlock[1][7] = largeBlock[2][8];
                largeBlock[0][8] = largeBlock[1][9];
                largeBlock[9][1] = temp1;
                largeBlock[8][2] = temp2;
                largeBlock[7][3] = temp3;
                largeBlock[6][4] = temp4;
                largeBlock[5][5] = temp5;
                largeBlock[4][6] = temp6;
                largeBlock[3][7] = temp7;
                largeBlock[2][8] = temp8;
                largeBlock[1][9] = temp9;
                
            }
            
            CipherProject.encryptField.setText(getCiphertext(largeBlock));
            
        }
    
        
    private void CTR(String binaryString, char[][] largeBlock, char[][] subBlock1, 
                char[][] subBlock2, char[][] subBlock3, char[][] subBlock4, String
                        subKey, String largeKey) {
        
        final String encryptKey = "100001010100101010101010010100011011010100101"
            + "001011010101001001000101111101010111010001010100110";
        
            StringBuilder BSappend = new StringBuilder(binaryString);
            while (BSappend.length() < 12) {
                BSappend.append(" ");
            }
            
            binaryString = BSappend.toString();
            
            sbIV.insert(0, "0000");
            
            StringBuilder ivXorKey = new StringBuilder();
            StringBuilder encryptResult = new StringBuilder();
            
            
            byte[] bytes = binaryString.getBytes();
            StringBuilder binary = new StringBuilder();
            for (byte b : bytes) {
                int val = b;
                for (int i = 0; i < 8; i++) {
                    binary.append((val & 128) == 0 ? 0 : 1);
                    val <<= 1;
                }
            }
            
            String appendedPlaintext = binary.toString();

            for(int i = 0; i < appendedPlaintext.length(); i++)
                ivXorKey.append((sbIV.charAt(i) ^ encryptKey.charAt(i)));
            
            for(int i = 0; i < appendedPlaintext.length(); i++)
                encryptResult.append((appendedPlaintext.charAt(i) ^ ivXorKey.charAt(i)));
            
            encryptResult.append(" " + " " + " " + " ");
            
            String cipherText = encryptResult.toString();
            
            // Populate the array
            int k = 0;
            for (int i = 0; i < largeBlock.length; i++) {
                for (int j = 0; j < largeBlock[i].length; j++) {
                    largeBlock[i][j] = cipherText.charAt(k);
                    k++;
                }
            }
        
        /*
                        0  1  1  0  0 | 0  0  1  0  1  
                        1  0  0  0  1 | 0  0  1  1  0  
       subBlock1 -->    0  0  1  1  0 | 1  1  0  0  1  <-- subBlock2
                        0  0  0  1  1 | 0  0  1  0  1  
                        0  1  1  0  0 | 1  1  0  0  1  
                        -----------------------------
                        1  0  0  1  1 | 1  0  1  1  0  
                        1  0  0  0  0 | 1  1  0  1  0  
       subBlock3 -->    0  1  0  1  1 | 0  1  0  1  0  <-- subBlock4
                        0  1  1  0  1 | 0  1  1  0  1  
                        1  0  1  1  0 | 0              
        */
        

            // Populate subBlock1
            for (int i = 0; i < subBlock1.length; i++) {
                for (int j = 0; j < subBlock1[i].length; j++) {
                    subBlock1[i][j] = largeBlock[i][j];

                }
            }

            // Populate subBlock2
            for (int i = 0; i < subBlock2.length; i++) {
                for (int j = 0; j < subBlock2[i].length; j++) {
                    subBlock2[i][j] = largeBlock[i][j + 5];

                }
            }

            // Populate subBlock3
            for (int i = 0; i < subBlock3.length; i++) {
                for (int j = 0; j < subBlock3[i].length; j++) {
                    subBlock3[i][j] = largeBlock[i + 5][j];

                }
            }

            // Populate subBlock4
            for (int i = 0; i < subBlock4.length; i++) {
                for (int j = 0; j < subBlock4[i].length; j++) {
                    subBlock4[i][j] = largeBlock[i + 5][j + 5];

                }
            }
            
            // Diagonal swap the subBlocks
            if ("09".equals(subKey.substring(0, 2))) {
                temp1 = subBlock1[0][0];
                subBlock1[0][0] = subBlock1[4][4];
                subBlock1[4][4] = temp1;
                
                temp1 = subBlock2[0][0];
                subBlock1[0][0] = subBlock1[4][4];
                subBlock1[4][4] = temp1;

                temp1 = subBlock3[0][0];
                subBlock1[0][0] = subBlock1[4][4];
                subBlock1[4][4] = temp1;

                temp1 = subBlock4[0][0];
                subBlock1[0][0] = subBlock1[4][4];
                subBlock1[4][4] = temp1;
                
                
            } else if ("08".equals(subKey.substring(2, 4))) {
                temp1 = subBlock1[0][1];
                temp2 = subBlock1[1][0];
                subBlock1[1][0] = subBlock1[4][3];
                subBlock1[0][1] = subBlock1[3][4];
                subBlock1[4][3] = temp1;
                subBlock1[3][4] = temp2;

                temp1 = subBlock2[0][1];
                temp2 = subBlock2[1][0];
                subBlock2[0][1] = subBlock2[4][3];
                subBlock2[1][0] = subBlock2[3][4];
                subBlock2[4][3] = temp1;
                subBlock2[3][4] = temp2;

                temp1 = subBlock3[0][1];
                temp2 = subBlock3[1][0];
                subBlock3[0][1] = subBlock3[4][3];
                subBlock3[1][0] = subBlock3[3][4];
                subBlock3[4][3] = temp1;
                subBlock3[3][4] = temp2;

                temp1 = subBlock4[0][1];
                temp2 = subBlock4[1][0];
                subBlock4[0][1] = subBlock4[4][3];
                subBlock4[1][0] = subBlock4[3][4];
                subBlock4[4][3] = temp1;
                subBlock4[3][4] = temp2;
                
            } else if ("07".equals(subKey.substring(4, 6))) {
                temp1 = subBlock1[2][0];
                temp2 = subBlock1[1][1];
                temp3 = subBlock1[0][2];
                subBlock1[2][0] = subBlock1[4][2];
                subBlock1[1][1] = subBlock1[3][3];
                subBlock1[0][2] = subBlock1[2][4];
                subBlock1[4][2] = temp1;
                subBlock1[3][3] = temp2;
                subBlock1[2][4] = temp3;
                
                temp1 = subBlock2[2][0];
                temp2 = subBlock2[1][1];
                temp3 = subBlock2[0][2];
                subBlock2[2][0] = subBlock2[4][2];
                subBlock2[1][1] = subBlock2[3][3];
                subBlock2[0][2] = subBlock2[2][4];
                subBlock2[4][2] = temp1;
                subBlock2[3][3] = temp2;
                subBlock2[2][4] = temp3;

                temp1 = subBlock3[2][0];
                temp2 = subBlock3[1][1];
                temp3 = subBlock3[0][2];
                subBlock3[2][0] = subBlock3[4][2];
                subBlock3[1][1] = subBlock3[3][3];
                subBlock3[0][2] = subBlock3[2][4];
                subBlock3[4][2] = temp1;
                subBlock3[3][3] = temp2;
                subBlock3[2][4] = temp3;

                temp1 = subBlock4[2][0];
                temp2 = subBlock4[1][1];
                temp3 = subBlock4[0][2];
                subBlock4[2][0] = subBlock4[4][2];
                subBlock4[1][1] = subBlock4[3][3];
                subBlock4[0][2] = subBlock4[2][4];
                subBlock4[4][2] = temp1;
                subBlock4[3][3] = temp2;
                subBlock4[2][4] = temp3;
                
            } else if ("06".equals(subKey.substring(6, 8))) {
                temp1 = subBlock1[3][0];
                temp2 = subBlock1[2][1];
                temp3 = subBlock1[1][2];
                temp4 = subBlock1[0][3];
                subBlock1[3][0] = subBlock1[4][1];
                subBlock1[2][1] = subBlock1[3][2];
                subBlock1[1][2] = subBlock1[2][3];
                subBlock1[0][3] = subBlock1[1][4];
                subBlock1[4][1] = temp1;
                subBlock1[3][2] = temp2;
                subBlock1[2][3] = temp3;
                subBlock1[1][4] = temp4;

                temp1 = subBlock2[3][0];
                temp2 = subBlock2[2][1];
                temp3 = subBlock2[1][2];
                temp4 = subBlock2[0][3];
                subBlock2[3][0] = subBlock2[4][1];
                subBlock2[2][1] = subBlock2[3][2];
                subBlock2[1][2] = subBlock2[2][3];
                subBlock2[0][3] = subBlock2[1][4];
                subBlock2[4][1] = temp1;
                subBlock2[3][2] = temp2;
                subBlock2[2][3] = temp3;
                subBlock2[1][4] = temp4;

                temp1 = subBlock3[3][0];
                temp2 = subBlock3[2][1];
                temp3 = subBlock3[1][2];
                temp4 = subBlock3[0][3];
                subBlock3[3][0] = subBlock3[4][1];
                subBlock3[2][1] = subBlock3[3][2];
                subBlock3[1][2] = subBlock3[2][3];
                subBlock3[0][3] = subBlock3[1][4];
                subBlock3[4][1] = temp1;
                subBlock3[3][2] = temp2;
                subBlock3[2][3] = temp3;
                subBlock3[1][4] = temp4;

                temp1 = subBlock4[3][0];
                temp2 = subBlock4[2][1];
                temp3 = subBlock4[1][2];
                temp4 = subBlock4[0][3];
                subBlock4[3][0] = subBlock4[4][1];
                subBlock4[2][1] = subBlock4[3][2];
                subBlock4[1][2] = subBlock4[2][3];
                subBlock4[0][3] = subBlock4[1][4];
                subBlock4[4][1] = temp1;
                subBlock4[3][2] = temp2;
                subBlock4[2][3] = temp3;
                subBlock4[1][4] = temp4;
            }
            
            // Diagonal swap the largeBlock
            if ("19".equals(largeKey.substring(0, 2))) {
                temp1 = largeBlock[0][0];
                largeBlock[0][0] = largeBlock[9][9];
                largeBlock[9][9] = temp1;
                
            } else if ("18".equals(largeKey.substring(2, 4))) {
                temp1 = largeBlock[1][0];
                temp2 = largeBlock[0][1];
                largeBlock[1][0] = largeBlock[9][8];
                largeBlock[0][1] = largeBlock[8][9];
                largeBlock[9][8] = temp1;
                largeBlock[8][9] = temp2;
                
            } else if ("17".equals(largeKey.substring(4, 6))) {
                temp1 = largeBlock[2][0];
                temp2 = largeBlock[1][1];
                temp3 = largeBlock[0][2];
                largeBlock[2][0] = largeBlock[9][7];
                largeBlock[1][1] = largeBlock[8][8];
                largeBlock[0][2] = largeBlock[7][9];
                largeBlock[9][7] = temp1;
                largeBlock[8][8] = temp2;
                largeBlock[7][9] = temp3;
                
            } else if ("16".equals(largeKey.substring(6, 8))) {
                temp1 = largeBlock[3][0];
                temp2 = largeBlock[2][1];
                temp3 = largeBlock[1][2];
                temp4 = largeBlock[0][3];
                largeBlock[3][0] = largeBlock[9][6];
                largeBlock[2][1] = largeBlock[8][7];
                largeBlock[1][2] = largeBlock[7][8];
                largeBlock[0][3] = largeBlock[6][9];
                largeBlock[9][6] = temp1;
                largeBlock[8][7] = temp2;
                largeBlock[7][8] = temp3;
                largeBlock[6][9] = temp4;
                
            } else if ("15".equals(largeKey.substring(8, 10))) {
                temp1 = largeBlock[4][0];
                temp2 = largeBlock[3][1];
                temp3 = largeBlock[2][2];
                temp4 = largeBlock[1][3];
                temp5 = largeBlock[0][4];
                largeBlock[4][0] = largeBlock[9][5];
                largeBlock[3][1] = largeBlock[8][6];
                largeBlock[2][2] = largeBlock[7][7];
                largeBlock[1][3] = largeBlock[6][8];
                largeBlock[0][4] = largeBlock[5][9];
                largeBlock[9][5] = temp1;
                largeBlock[8][6] = temp2;
                largeBlock[7][7] = temp3;
                largeBlock[6][8] = temp4;
                largeBlock[5][9] = temp5;
                
            } else if ("14".equals(largeKey.substring(10, 12))) {
                temp1 = largeBlock[5][0];
                temp2 = largeBlock[4][1];
                temp3 = largeBlock[3][2];
                temp4 = largeBlock[2][3];
                temp5 = largeBlock[1][4];
                temp6 = largeBlock[0][5];
                largeBlock[5][0] = largeBlock[9][4];
                largeBlock[4][1] = largeBlock[8][5];
                largeBlock[3][2] = largeBlock[7][6];
                largeBlock[2][3] = largeBlock[6][7];
                largeBlock[1][4] = largeBlock[5][8];
                largeBlock[0][5] = largeBlock[4][9];
                largeBlock[9][4] = temp1;
                largeBlock[8][5] = temp2;
                largeBlock[7][6] = temp3;
                largeBlock[6][7] = temp4;
                largeBlock[5][8] = temp5;
                largeBlock[4][9] = temp6;
                
            } else if ("13".equals(largeKey.substring(12, 14))) {
                temp1 = largeBlock[6][0];
                temp2 = largeBlock[5][1];
                temp3 = largeBlock[4][2];
                temp4 = largeBlock[3][3];
                temp5 = largeBlock[2][4];
                temp6 = largeBlock[1][5];
                temp7 = largeBlock[0][6];
                largeBlock[6][0] = largeBlock[9][3];
                largeBlock[5][1] = largeBlock[8][4];
                largeBlock[4][2] = largeBlock[7][5];
                largeBlock[3][3] = largeBlock[6][6];
                largeBlock[2][4] = largeBlock[5][7];
                largeBlock[1][5] = largeBlock[4][8];
                largeBlock[0][6] = largeBlock[3][9];
                largeBlock[9][3] = temp1;
                largeBlock[8][4] = temp2;
                largeBlock[7][5] = temp3;
                largeBlock[6][6] = temp4;
                largeBlock[5][7] = temp5;
                largeBlock[4][8] = temp6;
                largeBlock[3][9] = temp7;
                
            } else if ("12".equals(largeKey.substring(14, 16))) {
                temp1 = largeBlock[7][0];
                temp2 = largeBlock[6][1];
                temp3 = largeBlock[5][2];
                temp4 = largeBlock[4][3];
                temp5 = largeBlock[3][4];
                temp6 = largeBlock[2][5];
                temp7 = largeBlock[1][6];
                temp8 = largeBlock[0][7];
                largeBlock[7][0] = largeBlock[9][2];
                largeBlock[6][1] = largeBlock[8][3];
                largeBlock[5][2] = largeBlock[7][4];
                largeBlock[4][3] = largeBlock[6][5];
                largeBlock[3][4] = largeBlock[5][6];
                largeBlock[2][5] = largeBlock[4][7];
                largeBlock[1][6] = largeBlock[3][8];
                largeBlock[0][7] = largeBlock[2][9];
                largeBlock[9][2] = temp1;
                largeBlock[8][3] = temp2;
                largeBlock[7][4] = temp3;
                largeBlock[6][5] = temp4;
                largeBlock[5][6] = temp5;
                largeBlock[4][7] = temp6;
                largeBlock[3][8] = temp7;
                largeBlock[2][9] = temp8;
                
            } else if ("11".equals(largeKey.substring(16, 18))) {
                temp1 = largeBlock[8][0];
                temp2 = largeBlock[7][1];
                temp3 = largeBlock[6][2];
                temp4 = largeBlock[5][3];
                temp5 = largeBlock[4][4];
                temp6 = largeBlock[3][5];
                temp7 = largeBlock[2][6];
                temp8 = largeBlock[1][7];
                temp9 = largeBlock[0][8];
                largeBlock[8][0] = largeBlock[9][1];
                largeBlock[7][1] = largeBlock[8][2];
                largeBlock[6][2] = largeBlock[7][3];
                largeBlock[5][3] = largeBlock[6][4];
                largeBlock[4][4] = largeBlock[5][5];
                largeBlock[3][5] = largeBlock[4][6];
                largeBlock[2][6] = largeBlock[3][7];
                largeBlock[1][7] = largeBlock[2][8];
                largeBlock[0][8] = largeBlock[1][9];
                largeBlock[9][1] = temp1;
                largeBlock[8][2] = temp2;
                largeBlock[7][3] = temp3;
                largeBlock[6][4] = temp4;
                largeBlock[5][5] = temp5;
                largeBlock[4][6] = temp6;
                largeBlock[3][7] = temp7;
                largeBlock[2][8] = temp8;
                largeBlock[1][9] = temp9;
                
            }
            
            CipherProject.encryptField.setText(getCiphertext(largeBlock));
            
        }
        
    private void CBC(String binaryString, char[][] largeBlock, char[][] subBlock1, 
                char[][] subBlock2, char[][] subBlock3, char[][] subBlock4, String
                        subKey, String largeKey) {
        
        final String encryptKey = "101001010100011101101010010100011011010100101"
            + "000011010101101001010101111101010111011110010100100";
        
            StringBuilder BSappend = new StringBuilder(binaryString);
            while (BSappend.length() < 12) {
                BSappend.append(" ");
            }
            
            binaryString = BSappend.toString();
            
            sbIV.insert(0, "0000");

            StringBuilder ivXorPlain = new StringBuilder();
            StringBuilder encryptResult = new StringBuilder();
            
            byte[] bytes = binaryString.getBytes();
            StringBuilder binary = new StringBuilder();
            for (byte b : bytes) {
                int val = b;
                for (int i = 0; i < 8; i++) {
                    binary.append((val & 128) == 0 ? 0 : 1);
                    val <<= 1;
                }
            }
            
            String appendedPlaintext = binary.toString();

            
            for(int i = 0; i < appendedPlaintext.length(); i++)
                ivXorPlain.append((sbIV.charAt(i) ^ appendedPlaintext.charAt(i)));
            
            for(int i = 0; i < appendedPlaintext.length(); i++)
                encryptResult.append((ivXorPlain.charAt(i) ^ encryptKey.charAt(i)));
            
            encryptResult.append(" " + " " + " " + " ");
            
            String cipherText = encryptResult.toString();
            
            // Populate the array
            int k = 0;
            for (int i = 0; i < largeBlock.length; i++) {
                for (int j = 0; j < largeBlock[i].length; j++) {
                    largeBlock[i][j] = cipherText.charAt(k);
                    k++;
                }
            }
        
        /*
                        0  1  1  0  0 | 0  0  1  0  1  
                        1  0  0  0  1 | 0  0  1  1  0  
       subBlock1 -->    0  0  1  1  0 | 1  1  0  0  1  <-- subBlock2
                        0  0  0  1  1 | 0  0  1  0  1  
                        0  1  1  0  0 | 1  1  0  0  1  
                        -----------------------------
                        1  0  0  1  1 | 1  0  1  1  0  
                        1  0  0  0  0 | 1  1  0  1  0  
       subBlock3 -->    0  1  0  1  1 | 0  1  0  1  0  <-- subBlock4
                        0  1  1  0  1 | 0  1  1  0  1  
                        1  0  1  1  0 | 0              
        */
        

            // Populate subBlock1
            for (int i = 0; i < subBlock1.length; i++) {
                for (int j = 0; j < subBlock1[i].length; j++) {
                    subBlock1[i][j] = largeBlock[i][j];

                }
            }

            // Populate subBlock2
            for (int i = 0; i < subBlock2.length; i++) {
                for (int j = 0; j < subBlock2[i].length; j++) {
                    subBlock2[i][j] = largeBlock[i][j + 5];

                }
            }

            // Populate subBlock3
            for (int i = 0; i < subBlock3.length; i++) {
                for (int j = 0; j < subBlock3[i].length; j++) {
                    subBlock3[i][j] = largeBlock[i + 5][j];

                }
            }

            // Populate subBlock4
            for (int i = 0; i < subBlock4.length; i++) {
                for (int j = 0; j < subBlock4[i].length; j++) {
                    subBlock4[i][j] = largeBlock[i + 5][j + 5];

                }
            }
            
            // Diagonal swap the subBlocks
            if ("09".equals(subKey.substring(0, 2))) {
                temp1 = subBlock1[0][0];
                subBlock1[0][0] = subBlock1[4][4];
                subBlock1[4][4] = temp1;
                
                temp1 = subBlock2[0][0];
                subBlock1[0][0] = subBlock1[4][4];
                subBlock1[4][4] = temp1;

                temp1 = subBlock3[0][0];
                subBlock1[0][0] = subBlock1[4][4];
                subBlock1[4][4] = temp1;

                temp1 = subBlock4[0][0];
                subBlock1[0][0] = subBlock1[4][4];
                subBlock1[4][4] = temp1;
                
                
            } else if ("08".equals(subKey.substring(2, 4))) {
                temp1 = subBlock1[0][1];
                temp2 = subBlock1[1][0];
                subBlock1[1][0] = subBlock1[4][3];
                subBlock1[0][1] = subBlock1[3][4];
                subBlock1[4][3] = temp1;
                subBlock1[3][4] = temp2;

                temp1 = subBlock2[0][1];
                temp2 = subBlock2[1][0];
                subBlock2[0][1] = subBlock2[4][3];
                subBlock2[1][0] = subBlock2[3][4];
                subBlock2[4][3] = temp1;
                subBlock2[3][4] = temp2;

                temp1 = subBlock3[0][1];
                temp2 = subBlock3[1][0];
                subBlock3[0][1] = subBlock3[4][3];
                subBlock3[1][0] = subBlock3[3][4];
                subBlock3[4][3] = temp1;
                subBlock3[3][4] = temp2;

                temp1 = subBlock4[0][1];
                temp2 = subBlock4[1][0];
                subBlock4[0][1] = subBlock4[4][3];
                subBlock4[1][0] = subBlock4[3][4];
                subBlock4[4][3] = temp1;
                subBlock4[3][4] = temp2;
                
            } else if ("07".equals(subKey.substring(4, 6))) {
                temp1 = subBlock1[2][0];
                temp2 = subBlock1[1][1];
                temp3 = subBlock1[0][2];
                subBlock1[2][0] = subBlock1[4][2];
                subBlock1[1][1] = subBlock1[3][3];
                subBlock1[0][2] = subBlock1[2][4];
                subBlock1[4][2] = temp1;
                subBlock1[3][3] = temp2;
                subBlock1[2][4] = temp3;
                
                temp1 = subBlock2[2][0];
                temp2 = subBlock2[1][1];
                temp3 = subBlock2[0][2];
                subBlock2[2][0] = subBlock2[4][2];
                subBlock2[1][1] = subBlock2[3][3];
                subBlock2[0][2] = subBlock2[2][4];
                subBlock2[4][2] = temp1;
                subBlock2[3][3] = temp2;
                subBlock2[2][4] = temp3;

                temp1 = subBlock3[2][0];
                temp2 = subBlock3[1][1];
                temp3 = subBlock3[0][2];
                subBlock3[2][0] = subBlock3[4][2];
                subBlock3[1][1] = subBlock3[3][3];
                subBlock3[0][2] = subBlock3[2][4];
                subBlock3[4][2] = temp1;
                subBlock3[3][3] = temp2;
                subBlock3[2][4] = temp3;

                temp1 = subBlock4[2][0];
                temp2 = subBlock4[1][1];
                temp3 = subBlock4[0][2];
                subBlock4[2][0] = subBlock4[4][2];
                subBlock4[1][1] = subBlock4[3][3];
                subBlock4[0][2] = subBlock4[2][4];
                subBlock4[4][2] = temp1;
                subBlock4[3][3] = temp2;
                subBlock4[2][4] = temp3;
                
            } else if ("06".equals(subKey.substring(6, 8))) {
                temp1 = subBlock1[3][0];
                temp2 = subBlock1[2][1];
                temp3 = subBlock1[1][2];
                temp4 = subBlock1[0][3];
                subBlock1[3][0] = subBlock1[4][1];
                subBlock1[2][1] = subBlock1[3][2];
                subBlock1[1][2] = subBlock1[2][3];
                subBlock1[0][3] = subBlock1[1][4];
                subBlock1[4][1] = temp1;
                subBlock1[3][2] = temp2;
                subBlock1[2][3] = temp3;
                subBlock1[1][4] = temp4;

                temp1 = subBlock2[3][0];
                temp2 = subBlock2[2][1];
                temp3 = subBlock2[1][2];
                temp4 = subBlock2[0][3];
                subBlock2[3][0] = subBlock2[4][1];
                subBlock2[2][1] = subBlock2[3][2];
                subBlock2[1][2] = subBlock2[2][3];
                subBlock2[0][3] = subBlock2[1][4];
                subBlock2[4][1] = temp1;
                subBlock2[3][2] = temp2;
                subBlock2[2][3] = temp3;
                subBlock2[1][4] = temp4;

                temp1 = subBlock3[3][0];
                temp2 = subBlock3[2][1];
                temp3 = subBlock3[1][2];
                temp4 = subBlock3[0][3];
                subBlock3[3][0] = subBlock3[4][1];
                subBlock3[2][1] = subBlock3[3][2];
                subBlock3[1][2] = subBlock3[2][3];
                subBlock3[0][3] = subBlock3[1][4];
                subBlock3[4][1] = temp1;
                subBlock3[3][2] = temp2;
                subBlock3[2][3] = temp3;
                subBlock3[1][4] = temp4;

                temp1 = subBlock4[3][0];
                temp2 = subBlock4[2][1];
                temp3 = subBlock4[1][2];
                temp4 = subBlock4[0][3];
                subBlock4[3][0] = subBlock4[4][1];
                subBlock4[2][1] = subBlock4[3][2];
                subBlock4[1][2] = subBlock4[2][3];
                subBlock4[0][3] = subBlock4[1][4];
                subBlock4[4][1] = temp1;
                subBlock4[3][2] = temp2;
                subBlock4[2][3] = temp3;
                subBlock4[1][4] = temp4;
            }
            
            // Diagonal swap the largeBlock
            if ("19".equals(largeKey.substring(0, 2))) {
                temp1 = largeBlock[0][0];
                largeBlock[0][0] = largeBlock[9][9];
                largeBlock[9][9] = temp1;
                
            } else if ("18".equals(largeKey.substring(2, 4))) {
                temp1 = largeBlock[1][0];
                temp2 = largeBlock[0][1];
                largeBlock[1][0] = largeBlock[9][8];
                largeBlock[0][1] = largeBlock[8][9];
                largeBlock[9][8] = temp1;
                largeBlock[8][9] = temp2;
                
            } else if ("17".equals(largeKey.substring(4, 6))) {
                temp1 = largeBlock[2][0];
                temp2 = largeBlock[1][1];
                temp3 = largeBlock[0][2];
                largeBlock[2][0] = largeBlock[9][7];
                largeBlock[1][1] = largeBlock[8][8];
                largeBlock[0][2] = largeBlock[7][9];
                largeBlock[9][7] = temp1;
                largeBlock[8][8] = temp2;
                largeBlock[7][9] = temp3;
                
            } else if ("16".equals(largeKey.substring(6, 8))) {
                temp1 = largeBlock[3][0];
                temp2 = largeBlock[2][1];
                temp3 = largeBlock[1][2];
                temp4 = largeBlock[0][3];
                largeBlock[3][0] = largeBlock[9][6];
                largeBlock[2][1] = largeBlock[8][7];
                largeBlock[1][2] = largeBlock[7][8];
                largeBlock[0][3] = largeBlock[6][9];
                largeBlock[9][6] = temp1;
                largeBlock[8][7] = temp2;
                largeBlock[7][8] = temp3;
                largeBlock[6][9] = temp4;
                
            } else if ("15".equals(largeKey.substring(8, 10))) {
                temp1 = largeBlock[4][0];
                temp2 = largeBlock[3][1];
                temp3 = largeBlock[2][2];
                temp4 = largeBlock[1][3];
                temp5 = largeBlock[0][4];
                largeBlock[4][0] = largeBlock[9][5];
                largeBlock[3][1] = largeBlock[8][6];
                largeBlock[2][2] = largeBlock[7][7];
                largeBlock[1][3] = largeBlock[6][8];
                largeBlock[0][4] = largeBlock[5][9];
                largeBlock[9][5] = temp1;
                largeBlock[8][6] = temp2;
                largeBlock[7][7] = temp3;
                largeBlock[6][8] = temp4;
                largeBlock[5][9] = temp5;
                
            } else if ("14".equals(largeKey.substring(10, 12))) {
                temp1 = largeBlock[5][0];
                temp2 = largeBlock[4][1];
                temp3 = largeBlock[3][2];
                temp4 = largeBlock[2][3];
                temp5 = largeBlock[1][4];
                temp6 = largeBlock[0][5];
                largeBlock[5][0] = largeBlock[9][4];
                largeBlock[4][1] = largeBlock[8][5];
                largeBlock[3][2] = largeBlock[7][6];
                largeBlock[2][3] = largeBlock[6][7];
                largeBlock[1][4] = largeBlock[5][8];
                largeBlock[0][5] = largeBlock[4][9];
                largeBlock[9][4] = temp1;
                largeBlock[8][5] = temp2;
                largeBlock[7][6] = temp3;
                largeBlock[6][7] = temp4;
                largeBlock[5][8] = temp5;
                largeBlock[4][9] = temp6;
                
            } else if ("13".equals(largeKey.substring(12, 14))) {
                temp1 = largeBlock[6][0];
                temp2 = largeBlock[5][1];
                temp3 = largeBlock[4][2];
                temp4 = largeBlock[3][3];
                temp5 = largeBlock[2][4];
                temp6 = largeBlock[1][5];
                temp7 = largeBlock[0][6];
                largeBlock[6][0] = largeBlock[9][3];
                largeBlock[5][1] = largeBlock[8][4];
                largeBlock[4][2] = largeBlock[7][5];
                largeBlock[3][3] = largeBlock[6][6];
                largeBlock[2][4] = largeBlock[5][7];
                largeBlock[1][5] = largeBlock[4][8];
                largeBlock[0][6] = largeBlock[3][9];
                largeBlock[9][3] = temp1;
                largeBlock[8][4] = temp2;
                largeBlock[7][5] = temp3;
                largeBlock[6][6] = temp4;
                largeBlock[5][7] = temp5;
                largeBlock[4][8] = temp6;
                largeBlock[3][9] = temp7;
                
            } else if ("12".equals(largeKey.substring(14, 16))) {
                temp1 = largeBlock[7][0];
                temp2 = largeBlock[6][1];
                temp3 = largeBlock[5][2];
                temp4 = largeBlock[4][3];
                temp5 = largeBlock[3][4];
                temp6 = largeBlock[2][5];
                temp7 = largeBlock[1][6];
                temp8 = largeBlock[0][7];
                largeBlock[7][0] = largeBlock[9][2];
                largeBlock[6][1] = largeBlock[8][3];
                largeBlock[5][2] = largeBlock[7][4];
                largeBlock[4][3] = largeBlock[6][5];
                largeBlock[3][4] = largeBlock[5][6];
                largeBlock[2][5] = largeBlock[4][7];
                largeBlock[1][6] = largeBlock[3][8];
                largeBlock[0][7] = largeBlock[2][9];
                largeBlock[9][2] = temp1;
                largeBlock[8][3] = temp2;
                largeBlock[7][4] = temp3;
                largeBlock[6][5] = temp4;
                largeBlock[5][6] = temp5;
                largeBlock[4][7] = temp6;
                largeBlock[3][8] = temp7;
                largeBlock[2][9] = temp8;
                
            } else if ("11".equals(largeKey.substring(16, 18))) {
                temp1 = largeBlock[8][0];
                temp2 = largeBlock[7][1];
                temp3 = largeBlock[6][2];
                temp4 = largeBlock[5][3];
                temp5 = largeBlock[4][4];
                temp6 = largeBlock[3][5];
                temp7 = largeBlock[2][6];
                temp8 = largeBlock[1][7];
                temp9 = largeBlock[0][8];
                largeBlock[8][0] = largeBlock[9][1];
                largeBlock[7][1] = largeBlock[8][2];
                largeBlock[6][2] = largeBlock[7][3];
                largeBlock[5][3] = largeBlock[6][4];
                largeBlock[4][4] = largeBlock[5][5];
                largeBlock[3][5] = largeBlock[4][6];
                largeBlock[2][6] = largeBlock[3][7];
                largeBlock[1][7] = largeBlock[2][8];
                largeBlock[0][8] = largeBlock[1][9];
                largeBlock[9][1] = temp1;
                largeBlock[8][2] = temp2;
                largeBlock[7][3] = temp3;
                largeBlock[6][4] = temp4;
                largeBlock[5][5] = temp5;
                largeBlock[4][6] = temp6;
                largeBlock[3][7] = temp7;
                largeBlock[2][8] = temp8;
                largeBlock[1][9] = temp9;
                
            }
            
            CipherProject.encryptField.setText(getCiphertext(largeBlock));
            
        }
    
    private String getCiphertext(char[][] largeBlock) {
            StringBuilder ct = new StringBuilder();
            
            for (int i = 0; i < largeBlock.length; i++) {
                for (int j = 0; j < largeBlock[i].length; j++) {
                    ct.append(largeBlock[i][j]);
                }
            }
            
            String ciphertext = ct.toString();
            
            String output = "";
            for (int i = 0; i <= ciphertext.length() - 8; i += 8) {
                int k = Integer.parseInt(ciphertext.substring(i, i + 8), 2);
                output += (char) k;
            }
            
            return output;
        }
    
}
}