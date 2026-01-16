1.4

interface ProjecteurItf extends Remote {
  void setEcran( EcranItf ecran ) throws RemoteException;
  void play() throws RemoteException;
}
interface EcranItf extends Remote {
  void frame( byte[] data ) throws RemoteException;
  void run() throws RemoteException;
}






















--------------------


1.5 

class Projecteur extends UnicastRemoteObject implements ProjecteurItf {
  private byte[] film; 
  private EcranItf ecran;
  public Projecteur( byte[] film ) { this.film = film; }
  public void setEcran( EcranItf ecran ) { this.ecran = ecran; }
  pubic void play() {
    for( int i-0 ; i < film.length  ; i+=1024 ) {
      byte[] buf = System.arraycopy(film,i,buf,0,1024);
      ecran.frame(buf);
    }
    if( film.length % 1024 != 0 ) {  // dernière frame
      byte[] buf =
        System.arraycopy(film,(film.length/1024)*1024,buf,0,film.length%1024);
      ecran.frame(buf);
    }
    ecran.frame(new byte[]{});
} }










---------------------

1.7 
Avantage : performance ; Inconvénients : non garantie de livraison, plus compliqué à programmer 
void frame() {
  DatagramSocket s = new DatagramSocket(PORT);
  byte[] buf = new byte[1024];
  DatagramPacket p = new DatagramPacket(buf);
  s.receive(p);
  while( buf.length != 0 ) {
    // affichage des données contenues dans buf
    s.receive(p);
} }











-------------------
2.1 
CanalAboItf implémentée par les canaux. ConsoItf implémentée par les consommateurs.
interface CanalAboItf {
  void abonner( in ConsoItf c, in String ressourceId );
  void desabonner( in ConsoItf c, in String ressourceId );
}
----------------
2.2 
PushItf implémentée par les canaux et les consommateurs.
interface PushItf {
  void push( in String ressourceId, in String content );
}
------------------
2.3 
PullItf implémentée par les canaux et les producteurs.
typedef sequence<string> tstringarray;
interface PullItf {
  tstringarray pull( in String ressourceId );
}
-----------

2.4 
abonner -> PUT
desabonner -> DELETE
pull -> GET
push -> POST

-----------
2.5 
Push-pull: les producteurs font du push, les consommateurs font du pull, le canal sert de buffer.


------
2.6 

En ce qui concerne le point d’entrée, il faut pouvoir distinguer les appels qui proviennent du canal de ceux qui proviennent d’un pair. Ajout d’un paramètre à la méthode pull.

interface PullItf {
  tstringarray pull( in String ressourceId, in boolean init );
}

class PullImpl extends PullItfPOA {
  private PullItf vd;
  private isEntryPoint;  // valeur fixée par un constructeur
  public String[] pull( String id, boolean init ) {
    if( isEntryPoint && !init ) { return poll() ? get() : new String[0]; }
    return poll() ?
      get() + vd.pull(id,false) :  // + pour concaténation
      vd.pull(id,false);
  }
  private boolean poll() { return ... }
  private String get() { return ... }
}









