using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Navigation;
using Microsoft.Phone.Controls;
using Microsoft.Phone.Shell;

using Ajedrez.Ajedrez;
using Ajedrez.Piezas;

namespace Ajedrez.Vista
{
     public  class VistaTablero:Grid 
    {
        public static Tablero tablero;
        public static IPieza pieza;
        public static IJugador jugador;
        public Posicion posicion;

        public String jugador1;
        public String jugador2;
        //public JTextArea informacion = new JTextArea();
        //public JList historial;
        public static Boolean piezaPulsada = false;
        public static String seleccionAnterior;
        public static Posicion posicionAnterior, posicionActual;

        //Dependiendo del check nos dirá si vamos a jugar contra la máquina
        //o contra otro usuario

        public static Boolean contraPersona = false;
        public static Rect casilla[][] = new Rect[8][8];
        public Movimientos movimientos = new Movimientos();
        private DefaultListModel listModel;
        private Boolean turno = true;

        public VistaTablero(Tablero tab)
        {
            
            tablero = tab;
            InitializeComponent();
            setSize(600,500);
            
            tableroPanel.setLayout(new GridLayou(8, 8));
            seleccionAnterior = " ";
            posicionAnterior = new Posicion(0,0);
            posicionActual = new Posicion(0,0);

            final Maquina maq = new Maquina(tablero);

            // Dibujamos el tablero: cada casilla sera un Button.

            for (int i = 0; i < 8; i++) {
            final int fila = i;
            for (int j = 0; j < 8; j++) {
                final int columna = j;
                casilla[i][j] = new Button();
                casilla[i][j].setSize(new Dimension(50,50));
                casilla[fila][columna].setActionCommand("action"+i+j);
                //Comprobación de jugar con ordenador o persona

                contraMaquina.addItemListener(new ItemListener(){
                public void itemStateChanged(ItemEvent e) {
                    if(e.getStateChange() == ItemEvent.SELECTED){
                        TextBox.setEnabled(false);
                        TextBox.setText("Ordenador");
                        TextBox.setBackground(Color.LIGHT_GRAY);
                        contraPersona = false;
                    }
                    else if(e.getStateChange() == ItemEvent.DESELECTED){
                        TextBox.setEnabled(true);
                        TextBox.setText("jugador 2");
                        TextBox.setBackground(Color.White);
                        contraPersona = true;
                    }
                
                    validate();
                    repaint();
                }
  
            }; 
            
            casilla[i][j].addActionListener(new ActionListener() {
                    
                    public void actionPerformed(ActionEvent event) {
                        posicionActual.setFila(fila);
                        posicionActual.setColumna(columna);
                        Pieza piezaAnterior, piezaActual;
                        
                        if (turno){
                         
                        /* 
                          = Casilla vacia, sin pieza pulsada previamente
                         */
                        if(casilla[fila][columna].getIcon()==null && !piezaPulsada){
                            // 1. actualizamos posicionAnterior
                            // 2. piezaPulsada=true
                            // deseleccionamos la casilla
                            colorCasilla(casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()], posicionAnterior);
                            piezaPulsada=false;
                            posicionAnterior.setFila(posicionActual.getFila());
                            posicionAnterior.setColumna(posicionActual.getColumna());
                        }
                        /* 
                          = Casilla vacia, pieza pulsada previamente (movemos)
                         */
                        else if (casilla[fila][columna].getIcon()==null && piezaPulsada){
                            // 1. cogemos la pieza elegida previamente
                            // 2. actualizamos la informacion del panel
                            // 3. actualizamos el estado del tablero (el diccionario)
                            // 4. actualizamos la posicion de la pieza
                            // 5. actualizamos la visa
                            // 6. ponemos a false piezaPulsada
                            
                            piezaAnterior = tablero.comprobarPosicion(posicionAnterior);
                            
                            if (tablero.esMovimientoPosible(new Movimiento(piezaAnterior.color, piezaAnterior.posicion, posicionActual), piezaAnterior))
                            {
                                setEstado(posicionAnterior.toString()+" "+posicionActual.toString(), piezaAnterior + ": " + posicionAnterior.toString()+" "+posicionActual.toString(), event.getActionCommand());
                                movimientos.anadirMovimiento(piezaAnterior, new Movimiento(piezaAnterior.color, piezaAnterior.posicion, posicionActual));
                                if (turno)
                                    listModel.addElement(jugador1+": "+movimientos.getUltimoMovimiento());
                                else
                                    listModel.addElement(jugador2+": "+movimientos.getUltimoMovimiento());
                                
                                tablero.actualizarEstado(posicionAnterior, posicionActual);
                                
                                piezaAnterior.actualizarPosicion(posicionActual);
                                casilla[fila][columna].setIcon(casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()].getIcon());
                                casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()].setIcon(null);                             
                                // deseleccionamos la casilla
                                colorCasilla(casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()], posicionAnterior);
                                piezaPulsada=false;
                                posicionAnterior.setFila(posicionActual.getFila());
                                posicionAnterior.setColumna(posicionActual.getColumna());
                                
                                // comprobamos el jaque
                                tablero.comprobarJaque(piezaAnterior);

                                if(contraPersona == false){
                                Pieza p;
                                Movimiento m = maq.hacerMovimiento();
                                p=  tablero.estado.get((m.posActual).toString());
                                if (tablero.esMovimientoPosible(m,p)) 
                                {   
                                    if(tablero.estado.get(m.posDestino.toString())!=null){
                                        
                                        if (tablero.estado.get(m.posDestino.toString()).color != p.color)
                                        {
                                            //matar
                                            System.out.println("IA MATAf");
//                                            //matar(Posicion posicionAnterior, Posicion posicionActual, Pieza piezaAnterior,int fila, int columna);
//                                            matar(m.posActual, m.posDestino,p,m.posDestino.fila,m.posDestino.columna);
                                        }
                                    }
                                    setEstado(m.posActual.toString()+" "+posicionActual.toString(), piezaAnterior + ": " + m.posActual.toString()+" "+m.posDestino.toString(), event.getActionCommand());
                                    movimientos.anadirMovimiento(p, m);
                                    if (turno)
                                        listModel.addElement(jugador1+": "+movimientos.getUltimoMovimiento());
                                    else
                                        listModel.addElement(jugador2+": "+movimientos.getUltimoMovimiento());
                                    tablero.actualizarEstado(m.posActual, m.posDestino);
                                    
                                    //p.actualizarPosicion(m.posDestino);
                                    casilla[m.posDestino.fila][m.posDestino.columna].setIcon(casilla[m.posActual.getFila()][m.posActual.getColumna()].getIcon());
                                    casilla[m.posActual.getFila()][m.posActual.getColumna()].setIcon(null);                            
                                    // deseleccionamos la casilla
                                    colorCasilla(casilla[m.posActual.getFila()][m.posActual.getColumna()], m.posActual);
                                    piezaPulsada=false;
                                    m.posActual.setFila(m.posDestino.getFila());
                                    m.posActual.setColumna(m.posDestino.getColumna());
                                    turno = true;
                                }
                                }
                                else {
                                    System.out.println("Turno de las negras.");
                                    turno = false;
                                }
                            }
                            else {
                                System.out.println("No se puede actualizar la posicion");
                                System.out.println(">> "+tablero.estado.get(posicionAnterior.toString())+"\nMovimientos: "+tablero.estado.get(posicionAnterior.toString()).getMovimientosPosibles());
                                // deseleccionamos la casilla
                                colorCasilla(casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()], posicionAnterior);
                                piezaPulsada=false;
                                posicionAnterior.setFila(posicionActual.getFila());
                                posicionAnterior.setColumna(posicionActual.getColumna());
                                System.out.println("Turno de las blancas.");
                                turno = true;
                            }
                        }
                        
                        /* 
                          = Casilla llena, pieza no pulsada previamente
                         */
                        else if(casilla[fila][columna].getIcon()!=null && !piezaPulsada){
                            // 1. actualizamos posicionAnterior
                            // 2. piezaPulsada=true
                            // 3. coloreamos el fondo de la casilla
                            if(tablero.estado.get((new Posicion(fila,columna)).toString()).color == ajedrez.Color.blanca)
                            {
                                casilla[fila][columna].setBackground(Color.GRAY);                                
                                piezaPulsada=true;
                            }
                            posicionAnterior.setFila(posicionActual.getFila());
                            posicionAnterior.setColumna(posicionActual.getColumna());
                        }
                        
                        /* 
                          = Casilla llena, pieza pulsada previamente (matar?)
                         */
                        else if(casilla[fila][columna].getIcon()!=null && piezaPulsada){
                            // 1. mismo color -> nada
                            // 2. distinto color -> matar
                            piezaAnterior = tablero.comprobarPosicion(posicionAnterior);
                            piezaActual = tablero.comprobarPosicion(posicionActual);
                            if (piezaAnterior.color != piezaActual.color && tablero.esMovimientoPosible(new Movimiento(piezaAnterior.color, piezaAnterior.posicion, posicionActual), piezaAnterior)){
                                
                                /*
                                    intento implementar aquí el matar del peon
                                 */
                               
                                if(piezaAnterior.tipoPieza().equals("Peon")) 
                                {
                                     int f_aux, c_aux,x,y;
                                     f_aux = posicionAnterior.getFila();
                                     c_aux = posicionAnterior.getColumna();
                                     x = posicionActual.getFila();
                                     y = posicionActual.getColumna();
                                     /*También tengo que comprobar que el peón sólo mata hacia adelante,
                                      * es decir, dependiendo del color, tiene dos movimientos posibles
                                      * para matar
                                      */
                                     if(piezaAnterior.color == ajedrez.Color.blanca)
                                     {
                                         if((((x ==f_aux -1)&&( y==c_aux +1))) || ((( x== f_aux-1)&&(y ==c_aux -1))))
                                        {
                                            //Si se cumple ésta condición, el peón podrá matarlo
                                            matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
                                        }
                                     }
                                     else if (piezaAnterior.color == ajedrez.Color.negra)
                                     {
                                         if((((x == f_aux+1)&&(y == c_aux-1))) || (((x == f_aux+1)&&(y == c_aux+1))))
                                         {
                                                //Si se cumple ésta condición, el peón podrá matarlo
                                             matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
                                         }
                                     }                          
                                 }
                                if (piezaAnterior.tipoPieza().equals("Alfil"))
                                {
                                    int f_aux, c_aux,x,y;
                                    f_aux = posicionAnterior.getFila();
                                    c_aux = posicionAnterior.getColumna();
                                    x = posicionActual.getFila();
                                    y = posicionActual.getColumna();
                                    if (((x>= f_aux+1)&&(y>=c_aux+1)) || ((x>=f_aux+1)&&(y<=c_aux-1)) || ((x<=f_aux-1)&&(y<=c_aux-1)) || ((x<=f_aux-1)&&(y>=c_aux+1)))
                                    {
                                    if(Math.abs((y-c_aux)/(x-f_aux))==1)
                                        matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
                                    }
                                }
                                
                                if(piezaAnterior.tipoPieza().equals("Torre"))
                                {
                                    int f_aux, c_aux,x,y;
                                    f_aux = posicionAnterior.getFila();
                                    c_aux = posicionAnterior.getColumna();
                                    x = posicionActual.getFila();
                                    y = posicionActual.getColumna();
                                    if((x==f_aux) || (y==c_aux))
                                    {
                                        matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
                                    }
                                }
                                if(piezaAnterior.tipoPieza().equals("Reyna"))
                                {
                                    int f_aux, c_aux,x,y;
                                    f_aux = posicionAnterior.getFila();
                                    c_aux = posicionAnterior.getColumna();
                                    x = posicionActual.getFila();
                                    y = posicionActual.getColumna();
                                    try{
                                        if((x==f_aux) || (y==c_aux) || (Math.abs((y-c_aux)/(x-f_aux))==1))
                                        {
                                            matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
                                        }
                                    }catch(ArithmeticException e){
                                        
                                    }
                                }
                                if(piezaAnterior.tipoPieza().equals("Rey"))
                                {
                                    int f_aux, c_aux,x,y;
                                    f_aux = posicionAnterior.getFila();
                                    c_aux = posicionAnterior.getColumna();
                                    x = posicionActual.getFila();
                                    y = posicionActual.getColumna();
                                    if(((x==f_aux+1) && (y==c_aux+1)) || ((x==f_aux+1)&&(y==c_aux)) || ((x==f_aux+1)&&(y == c_aux-1)) || ((x==f_aux)&&(y==c_aux-1))
                                             || ((x==f_aux-1)&&(y==c_aux-1)) || ((x==f_aux -1 )&&(y==c_aux)) || ((x==f_aux-1)&&(y==c_aux+1)) || ((x==f_aux)&&(y==c_aux+1)))
                                    {
                                        matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
                                    }
                                }
                                if(piezaAnterior.tipoPieza().equals("Caballo"))
                                {
                                    int f_aux, c_aux,x,y;
                                    f_aux = posicionAnterior.getFila();
                                    c_aux = posicionAnterior.getColumna();
                                    x = posicionActual.getFila();
                                    y = posicionActual.getColumna();
                                    matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
                                }
                                
//                                else{
//                                   matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
//                            
//                                }
                                
                            // deseleccionamos la casilla
                            colorCasilla(casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()], posicionAnterior);
                            piezaPulsada=false; 
                            posicionAnterior.setFila(posicionActual.getFila());
                            posicionAnterior.setColumna(posicionActual.getColumna());
                            if(contraPersona == false){
                                    //estamos jugando contra el PC                                                            
                                    Pieza p;
                                    Movimiento m = maq.hacerMovimiento();
                                    p=  tablero.estado.get((m.posActual).toString());
                                    if (tablero.esMovimientoPosible(m,p)) 
                                    {   
                                        System.out.println("Color ficha: "+tablero.estado.get((m.posActual).toString()));
                                        if (tablero.estado.get(m.posActual.toString()).color != p.color)
                                        {
                                            //matar
                                            matar(m.posActual, m.posDestino,p,m.posDestino.fila,m.posDestino.columna);
                                        }
                                        setEstado(m.posActual.toString()+" "+posicionActual.toString(), piezaAnterior + ": " + m.posActual.toString()+" "+m.posDestino.toString(), event.getActionCommand());
                                        movimientos.anadirMovimiento(p, m);
                                        if (turno)
                                            listModel.addElement(jugador1+": "+movimientos.getUltimoMovimiento());
                                        else
                                            listModel.addElement(jugador2+": "+movimientos.getUltimoMovimiento());
                                        tablero.actualizarEstado(m.posActual, m.posDestino);
                                        //p.actualizarPosicion(posicionActual);
                                        casilla[m.posDestino.fila][m.posDestino.columna].setIcon(casilla[m.posActual.getFila()][m.posActual.getColumna()].getIcon());
                                        casilla[m.posActual.getFila()][m.posActual.getColumna()].setIcon(null);                            
                                        // deseleccionamos la casilla
                                        colorCasilla(casilla[m.posActual.getFila()][m.posActual.getColumna()], m.posActual);
                                        piezaPulsada=false;
                                        m.posActual.setFila(m.posDestino.getFila());
                                        m.posActual.setColumna(m.posDestino.getColumna());    
                                        System.out.println("Turno de las blancas.");
                                        turno = true;                                        
                                    }
                                }
                            else {
                                System.out.println("Turno de las negras.");
                                turno = false;
                            }    
                            }
                            else{
                                if(tablero.estado.get((new Posicion(fila,columna)).toString()).color == ajedrez.Color.blanca)
                                {
                                    // deseleccionamos la casilla
                                    colorCasilla(casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()], posicionAnterior);
                                    casilla[fila][columna].setBackground(Color.GRAY);                                
                                    piezaPulsada=true;
                                }
                                posicionAnterior.setFila(posicionActual.getFila());
                                posicionAnterior.setColumna(posicionActual.getColumna());
                            }
                            
                            
                        }                        
                    }
                    else{
                        if(casilla[fila][columna].getIcon()==null && !piezaPulsada){
                            // 1. actualizamos posicionAnterior
                            // 2. piezaPulsada=true
                            // deseleccionamos la casilla
                            colorCasilla(casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()], posicionAnterior);
                            piezaPulsada=false;
                            posicionAnterior.setFila(posicionActual.getFila());
                            posicionAnterior.setColumna(posicionActual.getColumna());
                        }
                        /* 
                          = Casilla vacia, pieza pulsada previamente (movemos)
                         */
                        else if (casilla[fila][columna].getIcon()==null && piezaPulsada){
                            // 1. cogemos la pieza elegida previamente
                            // 2. actualizamos la informacion del panel
                            // 3. actualizamos el estado del tablero (el diccionario)
                            // 4. actualizamos la posicion de la pieza
                            // 5. actualizamos la visa
                            // 6. ponemos a false piezaPulsada                            
                            piezaAnterior = tablero.comprobarPosicion(posicionAnterior);
                            
                            if (tablero.esMovimientoPosible(new Movimiento(piezaAnterior.color, piezaAnterior.posicion, posicionActual), piezaAnterior))
                            {
                                setEstado(posicionAnterior.toString()+" "+posicionActual.toString(), piezaAnterior + ": " + posicionAnterior.toString()+" "+posicionActual.toString(), event.getActionCommand());
                                movimientos.anadirMovimiento(piezaAnterior, new Movimiento(piezaAnterior.color, piezaAnterior.posicion, posicionActual));
                                if (turno)
                                    listModel.addElement(jugador1+": "+movimientos.getUltimoMovimiento());
                                else
                                    listModel.addElement(jugador2+": "+movimientos.getUltimoMovimiento());
                                tablero.actualizarEstado(posicionAnterior, posicionActual);
                                
                                piezaAnterior.actualizarPosicion(posicionActual);
                                casilla[fila][columna].setIcon(casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()].getIcon());
                                casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()].setIcon(null);                             
                                // deseleccionamos la casilla
                                colorCasilla(casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()], posicionAnterior);
                                piezaPulsada=false;
                                posicionAnterior.setFila(posicionActual.getFila());
                                posicionAnterior.setColumna(posicionActual.getColumna());
                                
                                if (tablero.comprobarJaque(piezaAnterior))
                                    System.out.println("El rey esta en JAQUE");
                                
                                System.out.println("Turno de las blancas.");
                                turno = true;
                                
                            }
                            else {
                                System.out.println("No se puede actualizar la posicion");
                                System.out.println(">> "+tablero.estado.get(posicionAnterior.toString())+"\nMovimientos: "+tablero.getMovimientosAlfil(tablero.estado.get(posicionAnterior.toString())));
                                // deseleccionamos la casilla
                                colorCasilla(casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()], posicionAnterior);
                                posicionAnterior.setFila(posicionActual.getFila());
                                posicionAnterior.setColumna(posicionActual.getColumna());
                                System.out.println("Turno de las negras.");
                                turno=false;
                                piezaPulsada=false;
                            }
                        }
                        
                        /* 
                          = Casilla llena, pieza no pulsada previamente
                         */
                        else if(casilla[fila][columna].getIcon()!=null && !piezaPulsada){
                            // 1. actualizamos posicionAnterior
                            // 2. piezaPulsada=true
                            // 3. coloreamos el fondo de la casilla
                            if(tablero.estado.get((new Posicion(fila,columna)).toString()).color == ajedrez.Color.negra){
                                casilla[fila][columna].setBackground(Color.GRAY);
                                piezaPulsada=true;
                            }
                            posicionAnterior.setFila(posicionActual.getFila());
                            posicionAnterior.setColumna(posicionActual.getColumna());
                        }
                        
                        /* 
                          = Casilla llena, pieza pulsada previamente (matar?)
                         */
                        else if(casilla[fila][columna].getIcon()!=null && piezaPulsada){
                            // 1. mismo color -> nada
                            // 2. distinto color -> matar
                            piezaAnterior = tablero.comprobarPosicion(posicionAnterior);
                            piezaActual = tablero.comprobarPosicion(posicionActual);
                            if (piezaAnterior.color != piezaActual.color && tablero.esMovimientoPosible(new Movimiento(piezaAnterior.color, piezaAnterior.posicion, posicionActual), piezaAnterior)){
                                
                                /*intento implementar aquí el matar del peon
                                 */
                               
                                if(piezaAnterior.tipoPieza().equals("Peon")) 
                                {// es un peon
                                     int f_aux, c_aux,x,y;
                                     f_aux = posicionAnterior.getFila();
                                     c_aux = posicionAnterior.getColumna();
                                     x = posicionActual.getFila();
                                     y = posicionActual.getColumna();
                                     /*También tengo que comprobar que el peón sólo mata hacia adelante,
                                      * es decir, dependiendo del color, tiene dos movimientos posibles
                                      * para matar
                                      */
                                     if(piezaAnterior.color == ajedrez.Color.blanca)
                                     {
                                         if((((x ==f_aux -1)&&( y==c_aux +1))) || ((( x== f_aux-1)&&(y ==c_aux -1))))
                                        {
                                            //Si se cumple ésta condición, el peón podrá matarlo
                                            matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
                                        }
                                     }
                                     else if (piezaAnterior.color == ajedrez.Color.negra)
                                     {
                                         if((((x == f_aux+1)&&(y == c_aux-1))) || (((x == f_aux+1)&&(y == c_aux+1))))
                                         {
                                             //Si se cumple ésta condición, el peón podrá matarlo
                                             matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
                                         }
                                     }                          
                                 }
                                if (piezaAnterior.tipoPieza().equals("Alfil"))
                                {
                                    int f_aux, c_aux,x,y;
                                     f_aux = posicionAnterior.getFila();
                                     c_aux = posicionAnterior.getColumna();
                                     x = posicionActual.getFila();
                                     y = posicionActual.getColumna();
                                     if (((x>= f_aux+1)&&(y>=c_aux+1)) || ((x>=f_aux+1)&&(y<=c_aux-1)) || ((x<=f_aux-1)&&(y<=c_aux-1)) || ((x<=f_aux-1)&&(y>=c_aux+1)))
                                     {
                                            if(Math.abs((y-c_aux)/(x-f_aux))==1)
                                            matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
                                    }
                                     
                                }
                                
                                if(piezaAnterior.tipoPieza().equals("Torre"))
                                {
                                    int f_aux, c_aux,x,y;
                                     f_aux = posicionAnterior.getFila();
                                     c_aux = posicionAnterior.getColumna();
                                     x = posicionActual.getFila();
                                     y = posicionActual.getColumna();
                                     if((x==f_aux) || (y==c_aux))
                                     {
                                         matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
                                     }
                                }
                                if(piezaAnterior.tipoPieza().equals("Reyna"))
                                {
                                    int f_aux, c_aux,x,y;
                                    f_aux = posicionAnterior.getFila();
                                    c_aux = posicionAnterior.getColumna();
                                    x = posicionActual.getFila();
                                    y = posicionActual.getColumna();
                                    try{
                                        if((x==f_aux) || (y==c_aux) || (Math.abs((y-c_aux)/(x-f_aux))==1))
                                        {
                                            matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
                                        }
                                    }catch(ArithmeticException e){
                                        
                                    }
                                }
                                if (piezaAnterior.tipoPieza().equals("Rey"))
                                {
                                    int f_aux, c_aux,x,y;
                                    f_aux = posicionAnterior.getFila();
                                    c_aux = posicionAnterior.getColumna();
                                    x = posicionActual.getFila();
                                    y = posicionActual.getColumna();
                                    if(((x==f_aux+1) && (y==c_aux+1)) || ((x==f_aux+1)&&(y==c_aux)) || ((x==f_aux+1)&&(y == c_aux-1)) || ((x==f_aux)&&(y==c_aux-1))
                                            || ((x==f_aux-1)&&(y==c_aux-1)) || ((x==f_aux -1 )&&(y==c_aux)) || ((x==f_aux-1)&&(y==c_aux+1)) || ((x==f_aux)&&(y==c_aux+1)))
                                    {
                                        matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
                                    }
                                     
                                }
                                if (piezaAnterior.tipoPieza().equals("Caballo"))
                                {
                                    int f_aux, c_aux,x,y;
                                    f_aux = posicionAnterior.getFila();
                                    c_aux = posicionAnterior.getColumna();
                                    x = posicionActual.getFila();
                                    y = posicionActual.getColumna();
                                    matar(posicionAnterior, posicionActual,piezaAnterior,fila,columna);
                                }
                                // deseleccionamos la casilla
                                colorCasilla(casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()], posicionAnterior);
                                piezaPulsada=false;
                                posicionAnterior.setFila(posicionActual.getFila());
                                posicionAnterior.setColumna(posicionActual.getColumna());
                                System.out.println("Turno de las blancas.");
                                turno = true;
                            }
                            
                            else{
                                if(tablero.estado.get((new Posicion(fila,columna)).toString()).color == ajedrez.Color.negra)
                                {
                                    // deseleccionamos la casilla
                                    colorCasilla(casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()], posicionAnterior);
                                    casilla[fila][columna].setBackground(Color.GRAY);                                
                                    piezaPulsada=true;
                                }
                                posicionAnterior.setFila(posicionActual.getFila());
                                posicionAnterior.setColumna(posicionActual.getColumna());
                            }
                        }                        
                        }
                    }
                });
                // pone el color a las casillas
                if ((i + j) % 2 == 0) {
                    casilla[i][j].setBackground(Color.white);
                } else {                    
                    casilla[i][j].setBackground(new Color(0x00a6ac));
                }   
                tableroPanel.add(casilla[i][j]);
                casilla[i][j].setEnabled(false);
            }
        }
        
        mostrarTablero();
        setVisible(true);        
    }
    public void colorCasilla(JButton boton, Posicion posicion){
        if ((posicion.getFila() + posicion.getColumna()) % 2 == 0) {
            boton.setBackground(Color.white);
        } else {                    
            boton.setBackground(new Color(0x00a6ac));
        } 
    }
    
    public void matar(Posicion posicionAnterior, Posicion posicionActual, Pieza piezaAnterior,int fila, int columna){
        tablero.estado.remove(posicionActual.toString()); // sacamos la pieza del diccionario
        casilla[posicionActual.getFila()][posicionActual.getColumna()].setIcon(null);
        tablero.actualizarEstado(posicionAnterior, posicionActual);
        piezaAnterior.actualizarPosicion(posicionActual);
        movimientos.anadirMovimiento(piezaAnterior, new Movimiento(piezaAnterior.color, piezaAnterior.posicion, posicionActual));
        if (turno)
            listModel.addElement(jugador1+": "+movimientos.getUltimoMovimiento());
        else
            listModel.addElement(jugador2+": "+movimientos.getUltimoMovimiento());
        casilla[fila][columna].setIcon(casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()].getIcon());
        casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()].setIcon(null);
        casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()].setBackground(casilla[posicionAnterior.getFila()][posicionAnterior.getColumna()].getBackground());
        tablero.comprobarJaque(piezaAnterior);
    }
    
    public void moverFicha(String seleccionActual, String seleccionAnterior)
    {
//        movimientos.anadirMovimiento("blancas", seleccionActual, seleccionAnterior);
//        tablero.ejecutarMovimiento(movimientos);
    }

    public final void mostrarTablero()
    {
        // Pasamos la posicion de las piezas a  la clase tablero
        tablero.colocarPiezas();
        // Colocamos las piezas en la vista                
        casilla[0][0].setIcon(new ImageIcon(VistaTablero.class.getResource("torren.png")));
        casilla[0][1].setIcon(new ImageIcon(VistaTablero.class.getResource("caballon.png")));
        casilla[0][2].setIcon(new ImageIcon(VistaTablero.class.getResource("alfiln.png")));
        casilla[0][3].setIcon(new ImageIcon(VistaTablero.class.getResource("reyn.png")));
        casilla[0][4].setIcon(new ImageIcon(VistaTablero.class.getResource("reinan.png")));
        casilla[0][5].setIcon(new ImageIcon(VistaTablero.class.getResource("alfiln.png")));
        casilla[0][6].setIcon(new ImageIcon(VistaTablero.class.getResource("caballon.png")));
        casilla[0][7].setIcon(new ImageIcon(VistaTablero.class.getResource("torren.png")));
        casilla[7][0].setIcon(new ImageIcon(VistaTablero.class.getResource("torreb.png")));
        casilla[7][1].setIcon(new ImageIcon(VistaTablero.class.getResource("caballob.png")));
        casilla[7][2].setIcon(new ImageIcon(VistaTablero.class.getResource("alfilb.png")));
        casilla[7][3].setIcon(new ImageIcon(VistaTablero.class.getResource("reyb.png")));
        casilla[7][4].setIcon(new ImageIcon(VistaTablero.class.getResource("reinab.png")));
        casilla[7][5].setIcon(new ImageIcon(VistaTablero.class.getResource("alfilb.png")));
        casilla[7][6].setIcon(new ImageIcon(VistaTablero.class.getResource("caballob.png")));
        casilla[7][7].setIcon(new ImageIcon(VistaTablero.class.getResource("torreb.png")));
        
        for (int i = 0; i < 8; i++) {
            casilla[1][i].setIcon(new ImageIcon(VistaTablero.class.getResource("peonn.png")));
            casilla[6][i].setIcon(new ImageIcon(VistaTablero.class.getResource("peonb.png")));
        }   
    }
    
    public void setEstado(String ultima, String turno, String pulsada){
        String estado = "Último movimiento: "+ultima+"\nJuegan "+turno+"\nSeleccionada: "+pulsada;
        informacion.setText(estado);
        validate();
        
        repaint();
        
    }
    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">                          
    private void initComponents() {

        statusPanel = new javax.swing.JPanel();
        j1TextField = new javax.swing.JTextField();
        jLabel1 = new javax.swing.JLabel();
        j2TextField = new javax.swing.JTextField();
        jLabel2 = new javax.swing.JLabel();
        jLabel3 = new javax.swing.JLabel();
        jSeparator1 = new javax.swing.JSeparator();
        jugarBoton = new javax.swing.JButton();
        contraMaquina = new javax.swing.JCheckBox();
        tableroPanel = new javax.swing.JPanel();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        j1TextField.setText("Jugador 1");
        j1TextField.setToolTipText("Nombre jugador 1");

        jLabel1.setFont(new java.awt.Font("Dialog", 1, 18)); // NOI18N
        jLabel1.setText("Master Chess");

        j2TextField.setText("Jugador 2");
        j2TextField.setToolTipText("Nombre jugador 2");

        jLabel2.setText("Blancas");

        jLabel3.setText("Negras");

        jugarBoton.setText("Jugar");
        jugarBoton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jugarBotonActionPerformed(evt);
            }
        });

        contraMaquina.setText("Contra ordenador");
        contraMaquina.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                contraMaquinaActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout statusPanelLayout = new javax.swing.GroupLayout(statusPanel);
        statusPanel.setLayout(statusPanelLayout);
        statusPanelLayout.setHorizontalGroup(
            statusPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(statusPanelLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(statusPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jSeparator1, javax.swing.GroupLayout.Alignment.TRAILING)
                    .addGroup(statusPanelLayout.createSequentialGroup()
                        .addGroup(statusPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addGroup(statusPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                                .addComponent(j1TextField)
                                .addComponent(jLabel1, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                .addComponent(j2TextField, javax.swing.GroupLayout.DEFAULT_SIZE, 138, Short.MAX_VALUE)
                                .addComponent(jLabel2)
                                .addComponent(jLabel3)
                                .addComponent(jugarBoton, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                            .addComponent(contraMaquina))
                        .addGap(0, 0, Short.MAX_VALUE)))
                .addContainerGap())
        );
        statusPanelLayout.setVerticalGroup(
            statusPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(statusPanelLayout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jLabel1)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(jSeparator1, javax.swing.GroupLayout.PREFERRED_SIZE, 10, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(jLabel2)
                .addGap(5, 5, 5)
                .addComponent(j1TextField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel3)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(j2TextField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(contraMaquina)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(jugarBoton)
                .addContainerGap(216, Short.MAX_VALUE))
        );

        getContentPane().add(statusPanel, java.awt.BorderLayout.LINE_START);

        javax.swing.GroupLayout tableroPanelLayout = new javax.swing.GroupLayout(tableroPanel);
        tableroPanel.setLayout(tableroPanelLayout);
        tableroPanelLayout.setHorizontalGroup(
            tableroPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 516, Short.MAX_VALUE)
        );
        tableroPanelLayout.setVerticalGroup(
            tableroPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 433, Short.MAX_VALUE)
        );

        getContentPane().add(tableroPanel, java.awt.BorderLayout.CENTER);

        pack();
    }// </editor-fold>                        

    private void jugarBotonActionPerformed(java.awt.event.ActionEvent evt) {                                           
        // Activamos los botones
        for (int i = 0; i < 8; i++) {
            for (int j = 0; j < 8; j++) {
                casilla[i][j].setEnabled(true);
            }
        }
        // Guardamos el nombre de los jugadores
        jugador1 = j1TextField.getText();
        jugador2 = j2TextField.getText();
        
        contraPersona=!contraMaquina.isSelected();
        
        /* 
            Cambiamos al panel de estadisticas: 
            - Ultimo movimiento: orig, dest
            - Turno de blancas/negras
        */
        remove(statusPanel);
        
        // nuevo panel con borde y titulo
        JPanel estado = new JPanel(new BorderLayout());
        
        // cuadro de texto que mostrara la informacion, con el color de fondo
        // del panel y que no sea editable por el usuario.
        informacion.setBorder(new TitledBorder("Jugadas"));
        informacion.setText("Comienza la partida");
        informacion.setBackground(this.getContentPane().getBackground());
        informacion.setEditable(false);
        
        // cuadro de texto con histórico de movimientos
        listModel = new DefaultListModel();
        historial = new JList(listModel);
        JScrollPane scrollPane = new JScrollPane();
        scrollPane.setViewportView(historial);
        historial.setAutoscrolls(true);
        historial.setBorder(new TitledBorder("Movimientos"));
        historial.setBackground(this.getContentPane().getBackground());
        
        estado.add(scrollPane, BorderLayout.CENTER);
        // añadimos el cuadro de texto al panel
        estado.add(informacion, BorderLayout.NORTH);        
        add(estado, BorderLayout.WEST);
        
        // refrescamos la vista
        validate();
        setVisible(true);
    } 
    }
}