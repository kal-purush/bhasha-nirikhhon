package main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import sistema.Buffer;
import sistema.Cliente;
import sistema.Servidor;

public class Main 
{
	private static Properties propiedades;
	private static Buffer bufferPrincipal;
	
	public static void main(String[] args) 
	{
		System.out.println("Cargando archivo properties.....");
		try 
		{
			propiedades = new Properties();
			propiedades.load(new FileInputStream("./data/info.properties"));
			System.out.println("Archivo properties cargado correctamente.");
		} 
		catch (FileNotFoundException e) 
		{
			System.out.println("Error, El archivo no exite");
		} 
		catch (IOException e) 
		{
			System.out.println("Error, No se puede leer el archivo");
		}
		
		//inicializa BufferPrincipal
		int nClientes = Integer.parseInt(propiedades.getProperty("numeroClientes"));
		bufferPrincipal = new Buffer(Integer.parseInt(propiedades.getProperty("tamanoBuffer")), nClientes);
		crearClientes();
		crearServidores();
	}
	
	public static void crearClientes()
	{
		System.out.println("Creando clientes...");
		int nClientes = Integer.parseInt(propiedades.getProperty("numeroClientes"));
		for(int i = 1; i <= nClientes; i++)
		{
			int nMensajes = Integer.parseInt(propiedades.getProperty("mnsCliente" + i));
			Cliente c = new Cliente(nMensajes, bufferPrincipal);
			System.out.println("cliente "+i);
			c.start();
		}
	}
	
	public static void crearServidores()
	{
		System.out.println("Creando servidores...");
		int nServidores = Integer.parseInt(propiedades.getProperty("numeroServidores"));
		for(int i = 0; i < nServidores; i++)
		{
			Servidor s = new Servidor(bufferPrincipal);
			s.start();
		}
	}

}