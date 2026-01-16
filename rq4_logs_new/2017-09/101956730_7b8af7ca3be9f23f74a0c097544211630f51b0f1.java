package servidor;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import mensajeria.PaqueteMovimiento;
import mensajeria.PaquetePersonaje;
import mundo.Tile;

public class ModuloNPC {

	public static void ejecutar() {
		cargarNPCs(); // Carga los NPCs cuando se inicia el servidor.

		Runnable mNPC = new Runnable() {
			public void run() {
					manejarNPCs(); // Le da directivas a los NPC.
			}
		};
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(mNPC, 0, 2, TimeUnit.SECONDS); // El código de mNPC se ejecuta cada 2 segundos.
	}

	private static float[] convertirEnIsometrico(int x, int y) {
		float[] iso = new float[2];

		iso[0] = (x - y) * (Tile.ANCHO / 2) + 2;
		iso[1] = (x + y) * (Tile.ALTO / 2) + 4;

		return iso;
	}

	private static void cargarNPCs() {
		cargarNPCsMapa1();
		// cargarNPCsMapa[N°]();...
	}
	
	private static void manejarNPCs() {
		manejarNPCsMapa1();
		// manejarNPCsMapa[N°]();...
	}

	/*** Mapa 1 ***/
	private static void cargarNPCsMapa1() {
		cargarTroll250();
		// cargar[Nombre][ID];...
	}
	
	private static void manejarNPCsMapa1() {
		manejarTroll250();
		// manejar[Nombre][ID];...
	}


	private static void cargarTroll250() {		
		PaqueteMovimiento pm = new PaqueteMovimiento();
		int j = 7;
		int i = 6;
		pm.setIdPersonaje(-250); // ID negativo para que no tenga conflictos con los usuarios.
		pm.setPosX(convertirEnIsometrico(j, i)[0]);
		pm.setPosY(convertirEnIsometrico(j, i)[1]);
		pm.setDireccion(5);
		pm.setFrame(0);
		pm.setIp("localhost");
		Servidor.getUbicacionPersonajes().put(pm.getIdPersonaje(), pm);

		PaquetePersonaje pp;
		try {
			pp = new PaquetePersonaje();
		} catch (IOException e) {
			pp = null;
			e.printStackTrace();
		}
		pp.setId(-250);
		pp.setMapa(1);
		pp.setEstado(1);
		pp.setCasta("Asesino");
		pp.setRaza("Orco");
		pp.setNombre("Troll [NPC]");
		pp.setSaludTope(5);
		pp.setEnergiaTope(5);
		pp.setFuerza(5);
		pp.setDestreza(1);
		pp.setInteligencia(5);
		pp.setNivel(1);
		pp.setExperiencia(0);
		pp.eliminarItems();
		pp.setIp("localhost");
		Servidor.getPersonajesConectados().put(pp.getId(), pp);
	}

	private static void manejarTroll250() {
		if (Servidor.getPersonajesConectados().get(-250).getEstado() == 2) // Pregunta si el NPC está en batalla.
		{			
			// Si es su turno, el NPC debería elegir un ataque.
		}
	}

}