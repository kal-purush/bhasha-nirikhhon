package GUI;

import java.awt.Color;
import java.awt.GridLayout;
import java.awt.Image;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JPanel;

import board.BoardGame;

public class BoardUI extends JPanel {
	private BoardGame boardBase;
	private JButton[][] board;
	private Image resize;
	
	
	//Ja recebe uma instancia de um tabuleiro base que será usado no jogo
	public BoardUI(BoardGame boardBase) {
		this.board = new JButton[8][8];
		this.boardBase = boardBase;
		this.createRealBoard();
		this.setColorBoard();

		this.setLayout(new GridLayout(8,8));
		this.setSize(800, 800);
		this.setLocation(50, 80);
		this.addBoard();
	}
	
	//Trata o tamanho da imagem para o jbutton
	public ImageIcon resizeImage(ImageIcon piece) {
		this.resize = piece.getImage(); //Pega a imagem salva
		Image newImage = resize.getScaledInstance(100, 100, Image.SCALE_SMOOTH);// Muda seu tamanho
		piece = new ImageIcon(newImage);//Poẽ na mesma instância de volta
		
		return piece;
		
	}
	
	//Metódo para instanciar os botões com base no tabuleiro base
	public void createRealBoard() {
		for(int i=0;i<8;i++) {
			for(int j=0;j<8;j++) {
				this.board[i][j] = new JButton();
				if(this.boardBase.getBoardMatrix()[i][j]!=null) {
					this.board[i][j].setIcon(this.resizeImage(this.boardBase.getBoardMatrix()[i][j].getImageIcon()));
				}
				
			}
		}
	}
	
	//Método para adicionar o tabuleiro para o JPanel e ao JFrame
	public void addBoard() {
		for(int i=0;i<8;i++) {
			for(int j=0;j<8;j++) {
					this.add(this.board[i][j]);	
			}
		}
			
	}
	
	//Setando a cor do tabuleiro
	public void setColorBoard() {
		for(int i=0;i<8;i++) {
			for(int j=0;j<8;j++) {
				if(i%2==0) {
					if(j%2==0) {
						this.board[i][j].setBackground(new Color(133,94,66));
					}
					else {
						this.board[i][j].setBackground(new Color(177,152,134));
					}
				}
				else {
					if(j%2==0) {
						this.board[i][j].setBackground(new Color(177,152,134));
					}
					else {
						this.board[i][j].setBackground(new Color(133,94,66));
					}
				}
			}
		}
	}
	
	
	public static void main(String[] args) {
		
	}
	
	//Getter e Setter
	public JButton[][] getBoard(){
		return this.board;
	}
	
	
	
}