package controller;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import model.Manutenzione;
import model.StrSel;
import model.Strumento;
import model.Utilizzo;

public class RecuperoDati implements Serializable {
	//dati utente
	private String user = "userlab05";
	private String passwd = "cinqueOP";

	//dati driver
	private String url = "jdbc:postgresql://dbserver.scienze.univr.it/dblab05";
	private String driver = "org.postgresql.Driver";

	//query	
	private String queryStrum = "SELECT s.cod, s.nome, d.nomed FROM strumento s FULL JOIN incarico d ON s.cod = d.cods";
	private  String queryDettStrum = "SELECT * FROM strumento WHERE cod = '";
	private String queryAllMan="SELECT * FROM Manutenzione"; //sovrascritta dopo � quella per ogni strumento
	private String queryUtilCrono="SELECT U.DataIn, U.DataF, U.motivo, U.NomeD, U.resp FROM UtStr U, Strumento S WHERE U.CodS=S.Cod ORDER BY U.DataIn DESC";
	private String newMan="INSERT INTO manutenzione VALUES ( '?' , '?' , '?' ,? , '?' , '?' ,? )";
	private String queryOpenman="SELECT * FROM Manutenzione"; //per visualizzare tutte le manutenzioni bisogna dirgli quelle aperte
	private String queryEditman=null;
	/*==========================costruttori====================================*/
	public RecuperoDati() throws ClassNotFoundException {
		Class.forName( driver );
	}

	/*==========================metodi====================================*/
	/*======BEAN=====*/
	//bean strumenti + collocazione
	private Strumento makeStrumBean( ResultSet rs ) throws SQLException {
		Strumento bean = new Strumento();
		bean.setNome( rs.getString( "nome" ) );
		bean.setCod( rs.getString( "cod"));
		String stato = rs.getString( "nomed" );
		if(stato==null) stato = "disponibile";
		bean.setNomed( stato );
		return bean;
	}

	private StrSel makeStrumSelBean( ResultSet rs ) throws SQLException {
		StrSel bean = new StrSel();
		bean.setCod(rs.getString( "cod"));
		bean.setNome(rs.getString( "nome"));
		bean.setWatt(rs.getFloat( "watt"));
		bean.setDitta(rs.getString( "ditta"));
		bean.setModello(rs.getString( "modello"));
		bean.setCosto(rs.getFloat( "costo"));
		bean.setAnnoaq(rs.getInt( "annoaq"));
		return bean;
	}

	private Utilizzo makeUtilCronoBean(ResultSet rs) throws SQLException{
		Utilizzo bean=new Utilizzo();
		bean.setDatain(rs.getString("datain"));
		bean.setDataf(rs.getString("dataf"));
		bean.setMotivo(rs.getString("motivo"));
		bean.setNomed(rs.getString("nomed"));
		bean.setResp(rs.getString("resp"));
		return bean;
	}

	private Manutenzione makeManutenzioneBean(ResultSet rs) throws SQLException{
		Manutenzione bean=new Manutenzione();
		bean.setCods(rs.getString( "cods"));
		bean.setData(rs.getString( "data"));
		bean.setDurata(rs.getString( "durata"));
		bean.setNumop(rs.getInt( "numop"));
		bean.setIditta(rs.getString( "iditta"));
		bean.setUrgenza(rs.getString( "urgenza"));
		bean.setCosto(rs.getFloat( "costo"));
		return bean;
	}

/*	private Carico makeCaricoBean() throws SQLException{
		Carico bean= new Carico();
		bean.setTesto("Viva la focaccia");
		return bean;
	}*/
	/*private Manutenzione makeManutenzioneBean(String risultato) throws SQLException{
		Manutenzione bean=new Manutenzione();
		bean.setTesto(risultato);
		return bean;
	}*/
	
	private Manutenzione makeOpenmanBean( ResultSet rs ) throws SQLException {
		Manutenzione bean = new Manutenzione();
		bean.setCods(rs.getString( "cods"));
		bean.setData(rs.getString( "data"));
		bean.setDurata(rs.getString( "durata"));
		bean.setNumop(rs.getInt( "numop"));
		bean.setIditta(rs.getString( "iditta"));
		bean.setUrgenza(rs.getString( "urgenza"));
		bean.setCosto(rs.getFloat( "costo"));
		return bean;
	}
	
	private Manutenzione makeEditmanBean( ResultSet rs ) throws SQLException {
		Manutenzione bean = new Manutenzione();
		bean.setCods(rs.getString( "cods"));
		bean.setData(rs.getString( "data"));
		bean.setDurata(rs.getString( "durata"));
		bean.setNumop(rs.getInt( "numop"));
		bean.setIditta(rs.getString( "iditta"));
		bean.setUrgenza(rs.getString( "urgenza"));
		bean.setCosto(rs.getFloat( "costo"));
		return bean;
	}

	/*=====FINE BEAN=====*/

	/*===RECUPERO DATI DA DATABASE===*/
	public String getUtente(String username, String password) {
		String queryUtenti = "Select id from persona where id='"+username+"' and psw='"+password+"'";
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		String result = "";
		try {
			// tentativo di connessione al database
			con = DriverManager.getConnection( url, user, passwd );
			// connessione riuscita, ottengo l'oggetto per l'esecuzione dell'interrogazione.
			stmt = con.createStatement();
			// eseguo l'interrogazione desiderata
			rs = stmt.executeQuery( queryUtenti );
			// memorizzo il risultato dell'interrogazione nel Vector
			if( rs.next() ) {
				result =  rs.getString("id");
			}
		} catch( SQLException sqle ) { // Catturo le eventuali eccezioni
			sqle.printStackTrace();

		} finally { // alla fine chiudo la connessione.
			try {
				con.close();
			} catch( SQLException sqle1 ) {
				sqle1.printStackTrace();
			}
		}
		return result;
	}

	public List<Manutenzione> getManutenzione(String cod) {

		queryAllMan="SELECT * FROM Manutenzione WHERE cods='"+cod+"' ORDER BY data DESC";
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		List<Manutenzione> result = new ArrayList<Manutenzione>();
		try {
			// tentativo di connessione al database
			con = DriverManager.getConnection( url, user, passwd );
			// connessione riuscita, ottengo l'oggetto per l'esecuzione dell'interrogazione.
			stmt = con.createStatement();
			// eseguo l'interrogazione desiderata
			rs = stmt.executeQuery( queryAllMan );
			// memorizzo il risultato dell'interrogazione nel Vector
			while( rs.next() ) {
				result.add( makeManutenzioneBean( rs ) );
			}
		} catch( SQLException sqle ) { // Catturo le eventuali eccezioni
			sqle.printStackTrace();

		} finally { // alla fine chiudo la connessione.
			try {
				con.close();
			} catch( SQLException sqle1 ) {
				sqle1.printStackTrace();
			}
		}
		return result;
	}

	public StrSel getStrumSel(String cod) {
		queryDettStrum = "SELECT * FROM strumento WHERE cod = '";
		queryDettStrum = queryDettStrum+cod+"'";
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		StrSel result = null;
		try {
			// tentativo di connessione al database
			con = DriverManager.getConnection( url, user, passwd );
			// connessione riuscita, ottengo l'oggetto per l'esecuzione dell'interrogazione.
			pstmt = con.prepareStatement( queryDettStrum );
			pstmt.clearParameters();
			// imposto i parametri della query
			//pstmt.setInt( 1, id );
			// eseguo la query
			rs = pstmt.executeQuery();
			// memorizzo il risultato dell'interrogazione in Vector di Bean
			if( rs.next() ) {
				result = makeStrumSelBean( rs );
			}

		} catch( SQLException sqle ) { // Catturo le eventuali eccezioni
			sqle.printStackTrace();

		} finally { // alla fine chiudo la connessione.
			try {
				con.close();
			} catch( SQLException sqle1 ) {
				sqle1.printStackTrace();
			}
		}
		return result;
	}

	public List<Utilizzo> getUtilizzi(String cod) {
		queryUtilCrono="SELECT U.DataIn, U.DataF, U.motivo, U.NomeD, U.resp FROM UtStr U WHERE U.CodS='"+cod+"' ORDER BY U.DataIn DESC";
		Connection con = null;
		PreparedStatement pstmt = null;
		Statement stmt = null;
		ResultSet rs = null;
		List<Utilizzo> result = new ArrayList<Utilizzo>();
		try {
			// tentativo di connessione al database
			con = DriverManager.getConnection( url, user, passwd );
			// connessione riuscita, ottengo l'oggetto per l'esecuzione dell'interrogazione.
			stmt = con.createStatement();
			// eseguo l'interrogazione desiderata
			rs = stmt.executeQuery( queryUtilCrono );
			// memorizzo il risultato dell'interrogazione nel Vector
			while( rs.next() ) {
				result.add( makeUtilCronoBean( rs ) );
			}

		} catch( SQLException sqle ) { // Catturo le eventuali eccezioni
			sqle.printStackTrace();

		} finally { // alla fine chiudo la connessione.
			try {
				con.close();
			} catch( SQLException sqle1 ) {
				sqle1.printStackTrace();
			}
		}
		return result;
	}

	public List<Strumento> getStrumentiDatabase() {
		// dichiarazione delle variabili
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		List<Strumento> result = new ArrayList<Strumento>();

		try {
			// tentativo di connessione al database
			con = DriverManager.getConnection( url, user, passwd );
			// connessione riuscita, ottengo l'oggetto per l'esecuzione dell'interrogazione.
			stmt = con.createStatement();
			// eseguo l'interrogazione desiderata
			rs = stmt.executeQuery( queryStrum );
			// memorizzo il risultato dell'interrogazione nel Vector
			while( rs.next() ) {
				result.add( makeStrumBean( rs ) );
			}

		} catch( SQLException sqle ) { // catturo le eventuali eccezioni!
			sqle.printStackTrace();

		} finally { // alla fine chiudo la connessione.
			try {
				con.close();
			} catch( SQLException sqle1 ) {
				sqle1.printStackTrace();
			}
		}
		return result;
	}
	
	public List<Manutenzione> getOpenman() {
		// dichiarazione delle variabili
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		List<Manutenzione> result = new ArrayList<Manutenzione>();

		try {
			// tentativo di connessione al database
			con = DriverManager.getConnection( url, user, passwd );
			// connessione riuscita, ottengo l'oggetto per l'esecuzione dell'interrogazione.
			stmt = con.createStatement();
			// eseguo l'interrogazione desiderata
			rs = stmt.executeQuery( queryOpenman );
			// memorizzo il risultato dell'interrogazione nel Vector
			while( rs.next() ) {
				result.add( makeOpenmanBean( rs ) );
			}

		} catch( SQLException sqle ) { // catturo le eventuali eccezioni!
			sqle.printStackTrace();

		} finally { // alla fine chiudo la connessione.
			try {
				con.close();
			} catch( SQLException sqle1 ) {
				sqle1.printStackTrace();
			}
		}
		return result;
	}
	/*===RECUPERO DATI DA DATABASE FINE===*/

	//query per inserire i dati nel database
	public boolean newManutenzione(Manutenzione nuovaman)  {
		// dichiarazione delle variabili
		Connection con = null;
		//PreparedStatement pstmt = null;
		Statement stmt = null;
		ResultSet rs = null;
		boolean res = false;
		//String result=null;
		String query="INSERT INTO manutenzione Values ('"+nuovaman.getCods()+"',"+"'"+nuovaman.getData()+"',"+"'"+nuovaman.getDurata()+"',"+"'"+nuovaman.getNumop()+"',"+"'"+nuovaman.getIditta()+"',"+"'"+nuovaman.getUrgenza()+"',"+"'"+nuovaman.getcosto()+"')";
		try {
			// tentativo di connessione al database
			con = DriverManager.getConnection( url, user, passwd );
			// connessione riuscita, ottengo l'oggetto per l'esecuzione dell'interrogazione.
			//pstmt = con.prepareStatement(this.newMan);
			//pstmt.clearParameters();
			stmt = con.createStatement();
			stmt.executeUpdate(query);
			res=true;
			// eseguo l'interrogazione desiderata
			/*rs = stmt.executeQuery( query );
			// memorizzo il risultato dell'interrogazione nel Vector
			if( rs.next() ) {
				result = makeManutenzioneBean( rs );
			}*/
			
			
			/*pstmt.setString(1, nuovaman.getCods());
			pstmt.setString(2, nuovaman.getData());
			pstmt.setString(3, nuovaman.getDurata());
			pstmt.setInt(4, nuovaman.getNumop());
			pstmt.setString(5, nuovaman.getIditta());
			pstmt.setString(6, nuovaman.getUrgenza());
			pstmt.setFloat(7, nuovaman.getcosto());*/

			// eseguo la query
			//pstmt.executeUpdate();

		} catch( SQLException sqle ) { // Catturo le eventuali eccezioni
			sqle.printStackTrace();

		} finally { // alla fine chiudo la connessione.
			try {
				con.close();
			} catch( SQLException sqle1 ) {
				sqle1.printStackTrace();
			}
			res=true;
		}
		
		return res;
	}
	
	public boolean newUtilizzo(Utilizzo nuovoutil)  {
		// dichiarazione delle variabili
		Connection con = null;
		//PreparedStatement pstmt = null;
		Statement stmt = null;
		ResultSet rs = null;
		boolean res = false;
		//String result=null;
		String query="INSERT INTO utstr Values ('"+nuovoutil.getDatain()+"',"+"'"+nuovoutil.getDataf()+"',"+"'"+nuovoutil.getMotivo()+"',"+"'"+nuovoutil.getResp()+"',"+"'"+nuovoutil.getCods()+"',"+"'"+nuovoutil.getNomed()+"')";
		try {
			// tentativo di connessione al database
			con = DriverManager.getConnection( url, user, passwd );
			// connessione riuscita, ottengo l'oggetto per l'esecuzione dell'interrogazione.
			//pstmt = con.prepareStatement(this.newMan);
			//pstmt.clearParameters();
			stmt = con.createStatement();
			stmt.executeUpdate(query);  //con update non genera l'eccezzione del dover ritornare qualcosa
			res=true;
			// eseguo l'interrogazione desiderata
			/*rs = stmt.executeQuery( query );
			// memorizzo il risultato dell'interrogazione nel Vector
			if( rs.next() ) {
				result = makeManutenzioneBean( rs );
			}*/
			
			
			/*pstmt.setString(1, nuovaman.getCods());
			pstmt.setString(2, nuovaman.getData());
			pstmt.setString(3, nuovaman.getDurata());
			pstmt.setInt(4, nuovaman.getNumop());
			pstmt.setString(5, nuovaman.getIditta());
			pstmt.setString(6, nuovaman.getUrgenza());
			pstmt.setFloat(7, nuovaman.getcosto());*/

			// eseguo la query
			//pstmt.executeUpdate();

		} catch( SQLException sqle ) { // Catturo le eventuali eccezioni
			sqle.printStackTrace();

		} finally { // alla fine chiudo la connessione.
			try {
				con.close();
			} catch( SQLException sqle1 ) {
				sqle1.printStackTrace();
			}
		}
		return res;
	}
	
	public Manutenzione setEditman(String cods, String data)  {
		// dichiarazione delle variabili
		queryEditman="SELECT cods, data, durata, numop, iditta, urgenza, costo FROM Manutenzione WHERE CodS='"+cods+"' AND data='"+data+"'";
		Connection con = null;
		//PreparedStatement pstmt = null;
		Statement stmt = null;
		ResultSet rs = null;
		Manutenzione res = new Manutenzione();
		//String result=null;
		try {
			// tentativo di connessione al database
			con = DriverManager.getConnection( url, user, passwd );
			// connessione riuscita, ottengo l'oggetto per l'esecuzione dell'interrogazione.
			//pstmt = con.prepareStatement(this.newMan);
			//pstmt.clearParameters();
			stmt = con.createStatement();
			stmt.executeQuery(queryEditman);
			if( rs.next() ) {
				res = makeEditmanBean( rs );
			}
		

		} catch( SQLException sqle ) { // Catturo le eventuali eccezioni
			sqle.printStackTrace();

		} finally { // alla fine chiudo la connessione.
			try {
				con.close();
			} catch( SQLException sqle1 ) {
				sqle1.printStackTrace();
			}
		}
		
		return res;
	}
	
}