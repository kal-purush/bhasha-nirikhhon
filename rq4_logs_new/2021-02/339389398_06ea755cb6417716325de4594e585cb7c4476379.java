/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sklep_gui;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Container;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.KeyStroke;
import warstwa_biznesowa.Fasada_warstwy_biznesowej;


/**
 * @author Paweł Esmanowicz
 */
public class GUI_main implements ActionListener {

       
    
    static JPanel cards;  	

    static CardLayout cl; 	//kolekcja paneli typu JPanel

       //utworzenie 4 paneli  reprezentujących formularze aplikacji
    static Pusty_form card0 = new Pusty_form();		//pusty formularz
    static Produkt_form card1 = new Produkt_form();	//panel formularza do wstawiania danych produktu
    static Produkty_form card2 = new Produkty_form();

    final static String PUSTY = "Pusty";
    final static String PRODUKT = "Dodaj auto";
    final static String PRODUKTY = "Lista aut";

    
    static Fasada_warstwy_biznesowej facade = new Fasada_warstwy_biznesowej();	//obiekt z warstwy biznesowej

    static public Fasada_warstwy_biznesowej getFacade() {
        return facade;
    }

    public JMenuBar createMenuBar() {
        JMenuBar menuBar;
        JMenu menu, submenu;
        JMenuItem menuItem;

        menuBar = new JMenuBar(); 			//tworzenie belki z glownymi .

        menu = new JMenu("A Menu");
        menu.setMnemonic(KeyEvent.VK_A);			//mozliwosc wyboru pozycji menu za pomocą klawiszy Alt-A
        menuBar.add(menu);		 		//dodanie pozycji menu do obiektu typu JMenuBar

        menuItem = new JMenuItem(PRODUKT, KeyEvent.VK_P);	//możliwosc wyboru opcji za pomocą klawiszy Alt-P
        menuItem.setAccelerator(KeyStroke.getKeyStroke(
                KeyEvent.VK_1, ActionEvent.ALT_MASK)); 	//możliwosc wyboru opcji za pomocą klawiszy Alt-1	
        menuItem.addActionListener(this);			// dodanie obsługi zdarzenia kliknięcia na pozycję JMenuItem
        menu.add(menuItem);				//dodanie pozycji menu do obiektu typu JMenu

        menuItem = new JMenuItem(PRODUKTY);
        menuItem.setMnemonic(KeyEvent.VK_R);    //możliwość wyboru opcji za pomocą klawiszy Alt-R – zamiast w konstruktorze JMenuItem
        menuItem.addActionListener(this);			// dodanie obsługi zdarzenia kliknięcia na pozycję JMenuItem
        menu.add(menuItem);				//dodanie pozycji menu do obiektu typu JMenu
        menuItem = new JMenuItem(PUSTY);
        menuItem.setMnemonic(KeyEvent.VK_U);	//mozliwosc wyboru opcji za pomocą klawiszy Alt-U
        menuItem.addActionListener(this);		// dodanie obsługi zdarzenia kliknięcia na pozycję JMenuItem
        menu.add(menuItem);			//dodanie pozycji menu do obiektu typu JMenu
        menu.addSeparator();

        submenu = new JMenu("A submenu"); 		//wykonanie do podrzędnego obiektu submenu typu JMenuJMenu 
        submenu.setMnemonic(KeyEvent.VK_A);	//mozliwosc wyboru opcji za pomocą klawiszy Alt-A

        menuItem = new JMenuItem(PUSTY);
        menuItem.setAccelerator(KeyStroke.getKeyStroke(
                KeyEvent.VK_2, ActionEvent.ALT_MASK));	//mozliwosc wyboru opcji za pomocą klawiszy Alt-2
        menuItem.addActionListener(this);		// dodanie obsługi zdarzenia kliknięcia na pozycję JMenuItem
        submenu.add(menuItem);		//dodanie pozycji menu do obiektu typu JMenu

        menuItem = new JMenuItem(PUSTY);
        menuItem.setMnemonic(KeyEvent.VK_S);	//mozliwosc wyboru opcji za pomocą klawiszy Alt-S
        menuItem.addActionListener(this);		// dodanie obsługi zdarzenia kliknięcia na pozycję JMenuItem
        submenu.add(menuItem);		//dodanie pozycji menu do obiektu typu JMenu

        menu.add(submenu);	           //Dodanie do nadrzędnego obiektu typu JMenu (menu) podrzędnego obiektu submenu typu JMenu

        menu = new JMenu("Inne Menu");  		//dodanie nowego menu typu JMenu w obiekcie typu JMenuBar
        menu.setMnemonic(KeyEvent.VK_I);		//mozliwosc wyboru opcji za pomocą klawiszy Alt-I
        menuBar.add(menu);			//dodanie pozycji menu do obiektu typu JMenuBar

        return menuBar;			//zwrocenie wykonanego komponentu typu JMenuBar
    }

    public Container createContentPane() {

        //  Pusty_form card0 = new Pusty_form();
        // Produkt_form card1 = new Produkt_form();
        card1.init();
        //Produkty_form card2 = new Produkty_form();
        card2.init();
        //wykonanie panelu cards do przechowania paneli typu Produkt_form, Produkty_form oraz Pusty_form
        cards = new JPanel(new CardLayout());
        cards.add(card0, PUSTY);		//dodanie panelu typu Pusty_form
        cards.add(card1, PRODUKT);		// dodanie panelu typu Produkt_form
        cards.add(card2, PRODUKTY);	// dodanie panelu typu Produkty_form

        JPanel p1 = new JPanel();		//utworzenie glownego panela aplikacji
        p1.add(cards, BorderLayout.CENTER);	//dodanie do glownego panelu zbioru cards innych paneli
        return p1;			// zwrócenie glownego panela aplikacji zawierającego kolekcję paneli
    }

    public static void updateDodaj_auto() {
        card2.table_content();		//wywołanie metody table_content panelu typu Produkty_form
        //do pobrania aktualnych danych o produktach z klasy Fasada_warstwy_biznesowej
        cl.show(cards, PRODUKTY);	//wyswietlenie panelu do wyswietlenia danych produktow

    }

    @Override
    public void actionPerformed(ActionEvent e) {

        JMenuItem source = (JMenuItem) (e.getSource()); 	//obsluga klikania na pozycje menu
        //   CardLayout cl = (CardLayout) (cards.getLayout());
        cl = (CardLayout) (cards.getLayout());
        switch (source.getText()) {
            case PRODUKT:
                cl.show(cards, PRODUKT);  		//wyswietlenie panelu do wprowadzania danych produktu
                break;
            case PRODUKTY:
                updateDodaj_auto(); 		//wyswietlenie panelu do  prezentacji danych produktow
                break;
            case PUSTY:
                cl.show(cards, PUSTY);		//wyswietlenie pustego panelu
                break;
            default:
                break;
        }
    }

    private static void createAndShowGUI() {		//metoda tworzaca okno typu JFrame i dodanie do niego 
        // obiekt  typu JMenuBar utworzony w metodzie createMenuBar oraz zbior 				// paneli w metodzie createContentPane
        JFrame frame = new JFrame("MenuDemo");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(900, 400);
        GUI_main demo = new GUI_main();
        frame.setJMenuBar(demo.createMenuBar());	//dodanie komponentu typu MenuBar do okna typu JFrame
        frame.setContentPane(demo.createContentPane());
        frame.setVisible(true);			 //wyswietlenie okna głównego typu JFrame

    }
      public static void main(String[] args) {
        //utworzenie wątku zarządzajcego zdarzeniemi utworzonego GUI ze zbiorem paneli reprezentujących formularze aplikacji

        java.awt.EventQueue.invokeLater(new Runnable() {
            @Override
            public void run() {
                createAndShowGUI();
            }
        });
    }

}