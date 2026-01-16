package ngspipesenginedemo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import ngspipesengine.configurator.engines.VMEngine;
import ngspipesengine.configurator.properties.VMProperties;
import ngspipesengine.exceptions.EngineException;
import ngspipesengine.utils.Utils;

import org.json.JSONException;

public class NGSPipesEngineConsole {

	static ServerSocket serverSocket;
	
	public static void main(String[] args) {				
				
		try {
			if(!VMEngine.acceptedVBoxVersion()) {
				System.out.println("Your current version of Virtual Box is to old.\n" +
						"Yo should have at least 4.3.20 version");
				System.exit(0);
			}
			VMEngine.register();

			switch(args[0]) {
				case "clean" : clean(args[1]);
				default : runEngine(args);
			}

		} catch(Exception ex) {
			System.out.println("::ERROR running engine :: \nstackTrace: " + Utils.getStackTrace(ex));
			System.exit(1);
		}
	}

	private static void runEngine(String[] args) throws JSONException, EngineException, IOException {		
		if(args.length < 4) {
			System.out.println(	"No especified needed arguments. {pipelinePath  input  output} [ engine name | - ]" + 
								"[ processor number | 0] [ memory | 0] [ from step] [ to step]");
			return;
		}
		VMProperties vmProperties = getVMProperties(args);
		VMEngine vm = new VMEngine(vmProperties);

		vm.start();
		initServer(vmProperties);
		
		System.out.println("Wait to finish engine");
		vm.finish();
	}

	private static void clean(String vmName) throws EngineException {
		try {
			VMEngine.clean(vmName);
		} catch(EngineException e) {
			throw new EngineException("Error cleaning engine", e);
		}

		System.out.println("VM " + vmName + " has been deleted");
	}

	private static VMProperties getVMProperties(String[] props) throws JSONException, EngineException, IOException {

		VMProperties propsVM = null;
		serverSocket =	new ServerSocket(0);

		if(props.length == 8)
			propsVM = new VMProperties(props[3], props[0], Integer.parseInt(props[6]), Integer.parseInt(props[7]));
		if(props.length == 7)
			propsVM = new VMProperties(props[3], props[0], Integer.parseInt(props[6]));
		if(props.length >= 4 && props.length < 7)
			propsVM = new VMProperties(props[3], props[0]);

		propsVM.setInputsPath(props[1]);
		propsVM.setOutputsPath(props[2]);
		propsVM.setPort(serverSocket.getLocalPort());
		
		if(props.length > 4) {
			propsVM.setProcessorsNumber(Integer.parseInt(props[4]));
			propsVM.setMemory(Integer.parseInt(props[5]) * 1024);
		}
		
		return propsVM;
	}

	public static void initServer(VMProperties vmProperties) throws IOException {

		try {
			Socket clientSocket = serverSocket.accept();   
			System.out.println("Connection accepted with: " + clientSocket.getInetAddress());
			
			BufferedReader in = new BufferedReader(
					new InputStreamReader(clientSocket.getInputStream()));
			
			String inputLine;
			while ((inputLine = in.readLine()) != null) 
				System.out.println(inputLine);
			
		} catch (IOException e) {
			System.out.println("Exception caught when trying to listen on port "
					+ serverSocket.getLocalPort() + " or listening for a connection");
			System.out.println(e.getMessage());
		} finally {
			if(!serverSocket.isClosed())
				serverSocket.close();
		}
	}	
}