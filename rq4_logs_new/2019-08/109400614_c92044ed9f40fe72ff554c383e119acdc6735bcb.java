/******************************************************************************
 * Copyright (c) 2000-2019 Ericsson Telecom AB
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/epl-2.0/EPL-2.0.html
 ******************************************************************************/
package org.eclipse.titan.runtime.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.titan.runtime.core.TTCN_Communication.transport_type_enum;
import org.eclipse.titan.runtime.core.TitanPort.Map_Params;
import org.eclipse.titan.runtime.core.TitanVerdictType.VerdictTypeEnum;
import org.eclipse.titan.runtime.core.cfgparser.CfgAnalyzer;
import org.eclipse.titan.runtime.core.cfgparser.ExecuteSectionHandler.ExecuteItem;
import org.eclipse.titan.runtime.core.cfgparser.MCSectionHandler;


/**
 * TODO: lots to implement
 *
 * @author Bianka Bekefi
 */

public class MainController {
	
	// TODO Use the messages from the TTCN_Communication class
	/* Any relation - any direction */

	private static final int MSG_ERROR = 0;

	/* Any relation - to MC (up) */

	private static final int MSG_LOG = 1;

	/* First messages - to MC (up) */

	/* from HCs */
	private static final int MSG_VERSION = 2;
	/* from MTC */
	private static final int MSG_MTC_CREATED = 3;
	/* from PTCs */
	private static final int MSG_PTC_CREATED = 4;

	/* Messages from MC to HC (down) */

	private static final int MSG_CREATE_MTC = 2;
	private static final int MSG_CREATE_PTC = 3;
	private static final int MSG_KILL_PROCESS = 4;
	private static final int MSG_EXIT_HC = 5;

	/* Messages from HC to MC (up) */

	private static final int MSG_CREATE_NAK = 4;
	private static final int MSG_HC_READY = 5;

	/* Messages from MC to TC (down) */

	private static final int MSG_CREATE_ACK = 1;
	private static final int MSG_START_ACK = 2;
	private static final int MSG_STOP = 3;
	private static final int MSG_STOP_ACK = 4;
	private static final int MSG_KILL_ACK = 5;
	private static final int MSG_RUNNING = 6;
	private static final int MSG_ALIVE = 7;
	private static final int MSG_DONE_ACK = 8;
	private static final int MSG_KILLED_ACK = 9;
	private static final int MSG_CANCEL_DONE = 10;
	private static final int MSG_COMPONENT_STATUS = 11;
	private static final int MSG_CONNECT_LISTEN = 12;
	private static final int MSG_CONNECT = 13;
	private static final int MSG_CONNECT_ACK = 14;
	private static final int MSG_DISCONNECT = 15;
	private static final int MSG_DISCONNECT_ACK = 16;
	private static final int MSG_MAP = 17;
	private static final int MSG_MAP_ACK = 18;
	private static final int MSG_UNMAP = 19;
	private static final int MSG_UNMAP_ACK = 20;

	/* Messages from MC to MTC (down) */

	private static final int MSG_EXECUTE_CONTROL = 21;
	private static final int MSG_EXECUTE_TESTCASE = 22;
	private static final int MSG_PTC_VERDICT = 23;
	private static final int MSG_CONTINUE = 24;
	private static final int MSG_EXIT_MTC = 25;

	/* Messages from MC to PTC (down) */

	private static final int MSG_START = 21;
	private static final int MSG_KILL = 22;

	/* Messages from TC to MC (up) */

	private static final int MSG_CREATE_REQ = 2;
	private static final int MSG_START_REQ = 3;
	private static final int MSG_STOP_REQ = 4;
	private static final int MSG_KILL_REQ = 5;
	private static final int MSG_IS_RUNNING = 6;
	private static final int MSG_IS_ALIVE = 7;
	private static final int MSG_DONE_REQ = 8;
	private static final int MSG_KILLED_REQ = 9;
	private static final int MSG_CANCEL_DONE_ACK = 10;
	private static final int MSG_CONNECT_REQ = 11;
	private static final int MSG_CONNECT_LISTEN_ACK = 12;
	private static final int MSG_CONNECTED = 13;
	private static final int MSG_CONNECT_ERROR = 14;
	private static final int MSG_DISCONNECT_REQ = 15;
	private static final int MSG_DISCONNECTED = 16;
	private static final int MSG_MAP_REQ = 17;
	private static final int MSG_MAPPED = 18;
	private static final int MSG_UNMAP_REQ = 19;
	private static final int MSG_UNMAPPED = 20;
	private static final int MSG_DEBUG_HALT_REQ = 101;
	private static final int MSG_DEBUG_CONTINUE_REQ = 102;
	private static final int MSG_DEBUG_BATCH = 103;

	/* Messages from MTC to MC (up) */

	private static final int MSG_TESTCASE_STARTED = 21;
	private static final int MSG_TESTCASE_FINISHED = 22;
	private static final int MSG_MTC_READY = 23;

	/* Messages from PTC to MC (up) */

	private static final int MSG_STOPPED = 21;
	private static final int MSG_STOPPED_KILLED = 22;
	private static final int MSG_KILLED = 23;

	/* Messages from MC to HC or TC (down) */

	private static final int MSG_DEBUG_COMMAND = 100;

	/* Messages from HC or TC to MC (up) */

	private static final int MSG_DEBUG_RETURN_VALUE = 100;

	/* Messages from MC to HC or MTC (down) */

	private static final int MSG_CONFIGURE = 200;

	/* Messages from HC or MTC to MC (up) */

	private static final int MSG_CONFIGURE_ACK = 200;
	private static final int MSG_CONFIGURE_NAK = 201;
	
	private enum mcStateEnum{ MC_INACTIVE, MC_LISTENING, MC_LISTENING_CONFIGURED, MC_HC_CONNECTED,
	  MC_CONFIGURING, MC_ACTIVE, MC_SHUTDOWN, MC_CREATING_MTC, MC_READY,
	  MC_TERMINATING_MTC, MC_EXECUTING_CONTROL, MC_EXECUTING_TESTCASE,
	  MC_TERMINATING_TESTCASE, MC_PAUSED, MC_RECONFIGURING
	}
	
	private enum connStateEnum { CONN_LISTENING, CONN_CONNECTING, CONN_CONNECTED,
		  CONN_DISCONNECTING, CONN_MAPPING, CONN_MAPPED, CONN_UNMAPPING
	}
	
	private enum hcStateEnum { HC_IDLE, HC_CONFIGURING, HC_ACTIVE, HC_OVERLOADED, HC_CONFIGURING_OVERLOADED, 
		HC_EXITING, HC_DOWN
	}
	
	private static mcStateEnum mc_state;
	
	private static ThreadLocal<Text_Buf> incoming_buf = new ThreadLocal<Text_Buf>() {
		@Override
		protected Text_Buf initialValue() {
			return new Text_Buf();
		}
	};
	

	private static ThreadLocal<BigInteger> n_hosts = new ThreadLocal<BigInteger>() {
		@Override
		protected BigInteger initialValue() {
			return BigInteger.ZERO;
		}
	};
	
	private static ThreadLocal<String> config_str = new ThreadLocal<String>() {
		@Override
		protected String initialValue() {
			return null;
		}
	}; 
	
	private static ThreadLocal<Boolean> any_component_done_requested = new ThreadLocal<Boolean>() {
		@Override
		protected Boolean initialValue() {
			return false;
		}
	};
	
	private static ThreadLocal<Boolean> any_component_done_sent = new ThreadLocal<Boolean>() {
		@Override
		protected Boolean initialValue() {
			return false;
		}
	};
	
	private static ThreadLocal<Boolean> all_component_done_requested = new ThreadLocal<Boolean>() {
		@Override
		protected Boolean initialValue() {
			return false;
		}
	};
	
	private static ThreadLocal<Boolean> any_component_killed_requested = new ThreadLocal<Boolean>() {
		@Override
		protected Boolean initialValue() {
			return false;
		}
	};
	
	private static ThreadLocal<Boolean> all_component_killed_requested = new ThreadLocal<Boolean>() {
		@Override
		protected Boolean initialValue() {
			return false;
		}
	};
	
	private static ThreadLocal<Boolean> stop_requested = new ThreadLocal<Boolean>() {
		@Override
		protected Boolean initialValue() {
			return false;
		}
	};
	
	private static ThreadLocal<Boolean> stop_after_tc = new ThreadLocal<Boolean>() {
		@Override
		protected Boolean initialValue() {
			return false;
		}
	};
	
	private static int next_comp_ref;
	private static int tc_first_comp_ref;
	private static List<Host> hosts;
	private static List<ExecuteItem> executeItems;
	private static ServerSocketChannel serverSocketChannel;
	
	private static ThreadLocal<CfgAnalyzer> cfgAnalyzer = new ThreadLocal<CfgAnalyzer>() {
		@Override
		protected CfgAnalyzer initialValue() {
			return null;
		}
	};
	
	private static Map<Integer, ComponentStruct> components;
	
	static class Host {
		SocketChannel socket;
		SocketAddress address;
		hcStateEnum hc_state;
		List<ComponentStruct> components;
		boolean transport_supported[];
		String hostname;
		String machine_type;
		String system_name;
		String system_release;
		String system_version;
		
		Host(SocketChannel sc) {
			socket = sc;
			components = new ArrayList<ComponentStruct>();
			transport_supported = new boolean[transport_type_enum.TRANSPORT_NUM.ordinal()];
		}
		
		void addComponent(ComponentStruct comp) {
			components.add(comp);
		}
		
	}
	
	private enum tcStateEnum { TC_INITIAL, TC_IDLE, TC_CREATE, TC_START, TC_STOP, TC_KILL,
		  TC_CONNECT, TC_DISCONNECT, TC_MAP, TC_UNMAP, TC_STOPPING, TC_EXITING,
		  TC_EXITED,
		  MTC_CONTROLPART, MTC_TESTCASE, MTC_ALL_COMPONENT_STOP,
		  MTC_ALL_COMPONENT_KILL, MTC_TERMINATING_TESTCASE, MTC_PAUSED,
		  PTC_FUNCTION, PTC_STARTING, PTC_STOPPED, PTC_KILLING, PTC_STOPPING_KILLING,
		  PTC_STALE, TC_SYSTEM, MTC_CONFIGURING 
	};

	
	static class ComponentStruct {
		int comp_ref;
		QualifiedName comp_type;
		String comp_name;
		String log_source;
		Host comp_location;
		tcStateEnum tc_state;
		VerdictTypeEnum local_verdict;
		String verdict_reason;
		int tc_fd;
		Text_Buf text_buf;
		QualifiedName tc_fn_name;
		String return_type;
		byte[] return_value;
		boolean is_alive;
		boolean stop_requested;
		boolean process_killed;
		// int arg;
		byte[] arg;
		ComponentStruct create_requestor;
		ComponentStruct start_requestor;
		RequestorStruct cancel_done_sent_to;
		RequestorStruct stop_requestors;
		RequestorStruct kill_requestors;
		List<PortConnection> conn_head_list;
		List<PortConnection> conn_tail_list;
		RequestorStruct done_requestors;
		RequestorStruct killed_requestors;
		RequestorStruct cancel_done_sent_for;
		
		ComponentStruct(final Text_Buf tb) {
			text_buf = tb;
		}
		
	}
	
	static class HostGroupStruct {
		String group_name;
		boolean has_all_hosts;
		boolean has_all_components;
		List<Host> host_members;
		List<ComponentStruct> assigned_components;
	}
	
	static class TimerStruct {
		double expiration;
		ComponentStruct component;
	}
	
	
	static class QualifiedName {
		public String module_name;
		public String definition_name;

		public QualifiedName(final String module_name, final String definition_name) {
			this.module_name = module_name;
			this.definition_name = definition_name;
		}
		
	}
	
	static class PortConnection {
		connStateEnum conn_state;
		transport_type_enum transport_type;
		int comp_ref;
		String port_name;
		PortConnection next;
		PortConnection prev;
		ComponentStruct component;
		List<ComponentStruct> components;
		int headComp;
		String headPort;
		int tailComp;
		String tailPort;
		RequestorStruct requestors;		 
	}
	
	
	static class RequestorStruct {
		int n_components;
		ComponentStruct comp;
		List<ComponentStruct> components;
	}
	
	private static ComponentStruct mtc;
	private static ComponentStruct ptc;
	private static ComponentStruct system;
	
	public static void main(String[] args) {
		if(args.length > 1) {
			System.out.println("For now only 1 arguments can be passed, the config file.");
		}
		
		printWelcome();

		if(args.length == 1) {
			mc_state = mcStateEnum.MC_INACTIVE;
			final File config_file = new File(args[0]);
			System.out.println("Using configuration file: "+config_file.getName());
			cfgAnalyzer.set(new CfgAnalyzer()); 
			final boolean config_file_failure = cfgAnalyzer.get().parse(config_file);
			try {
				config_str.set(new Scanner(config_file).useDelimiter("\\Z").next());
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			MCSectionHandler mcSectionHandler = cfgAnalyzer.get().getMcSectionHandler();
			
			executeItems = cfgAnalyzer.get().getExecuteSectionHandler().getExecuteitems();
			if (config_file_failure) {
				System.out.println("Error was found in the configuration file.");
			}
			else {
				n_hosts.set(mcSectionHandler.getNumHCsText());
				
				try {
					serverSocketChannel = ServerSocketChannel.open();
					serverSocketChannel.socket().bind(new InetSocketAddress(mcSectionHandler.getLocalAddress(), 
							mcSectionHandler.getTcpPort().intValue()));					
					// FIXME BigInteger to int
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}

		if (n_hosts.get().compareTo(BigInteger.ZERO) <= 0) {
			// TODO interactiveMode
		}
		else {
			batchMode();
		}
		// TODO cleanUp()
			
	}
	
	private static int batchMode() {
		next_comp_ref = TitanComponent.FIRST_PTC_COMPREF;
		System.out.println(String.format("Entering batch mode. Waiting for %d HC%s to connect...", n_hosts.get(), 
				 n_hosts.get().compareTo(BigInteger.ONE) > 0 ? "s" : ""));
		
		if (executeItems.size() <= 0) {
		    // TODO return 
		    return -1;
		}
		mc_state = mcStateEnum.MC_LISTENING;
		try {
			hosts = new ArrayList<Host>();
			System.out.println(MessageFormat.format("Listening on IP address {0} and TCP port {1}.", 
					cfgAnalyzer.get().getMcSectionHandler().getLocalAddress(), cfgAnalyzer.get().getMcSectionHandler().getTcpPort().toString()));
			while(n_hosts.get().compareTo(BigInteger.valueOf(hosts.size())) > 0) {
				SocketChannel sc = serverSocketChannel.accept();
				Host host = new Host(sc);
				hosts.add(host);
				host.address = sc.getRemoteAddress();				
				host.hc_state = hcStateEnum.HC_IDLE;
				
				// TODO receive USAGE_STAT
				process_version(host);	
				System.out.println(MessageFormat.format("New HC connected from {0} [{1}]. : {2} {3} on {4}.", host.hostname, host.socket.getRemoteAddress().toString(), host.system_name, host.system_release, 
						host.machine_type)); 
			}

			mc_state = mcStateEnum.MC_ACTIVE;
			configure();
			create_mtc(hosts.get(0));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return 0;
	}
	
	private static void create_mtc(Host host) {		
		if(mc_state != mcStateEnum.MC_ACTIVE) {
			// TODO error
			return;
		}
		
		switch (host.hc_state) {
		case HC_OVERLOADED:
			System.out.println("HC on host"+host.hostname+" reported overload. Trying to create MTC there anyway.");
		case HC_ACTIVE:
			break;
		default:
			// TODO error
		    return;
		} 
		
		System.out.println("Creating MTC on host "+host.hostname+".");
		send_create_mtc(host);

		mtc = new ComponentStruct(new Text_Buf());
		mtc.comp_ref = TitanComponent.MTC_COMPREF;
		mtc.comp_name = "MTC";
		mtc.tc_state = tcStateEnum.TC_INITIAL;
		mtc.local_verdict = VerdictTypeEnum.NONE;
		mtc.comp_location = host;
		host.addComponent(mtc);
		components = new HashMap<Integer, ComponentStruct>();
		components.put(mtc.comp_ref, mtc);
		mtc.done_requestors = init_requestors(null);
		mtc.killed_requestors = init_requestors(null);
		mtc.cancel_done_sent_for = init_requestors(null);
		mtc.conn_head_list = new ArrayList<PortConnection>();
		mtc.conn_tail_list = new ArrayList<PortConnection>();	

		system = new ComponentStruct(new Text_Buf());
		system.comp_ref = TitanComponent.SYSTEM_COMPREF;
		system.comp_name = "SYSTEM";
		system.tc_state = tcStateEnum.TC_SYSTEM;
		system.local_verdict = VerdictTypeEnum.NONE;
		components.put(system.comp_ref, system);
		system.done_requestors = init_requestors(null);
		system.killed_requestors = init_requestors(null);
		system.cancel_done_sent_for = init_requestors(null);
		system.conn_head_list = new ArrayList<PortConnection>();
		system.conn_tail_list = new ArrayList<PortConnection>();
		
		mc_state = mcStateEnum.MC_CREATING_MTC;	
		
		final Thread MTC = new Thread() {
			
			@Override
			public void run() {
				connect_mtc();
			}
			
		};

		MTC.start();
	}
	
	private static void connect_mtc() {
		SocketChannel sc;
		try {
			sc = serverSocketChannel.accept();
			Host mtcHost = new Host(sc);
			mtcHost.address = sc.getLocalAddress();
			setup_host(mtcHost);
			mtc.comp_location = mtcHost;
			handle_unknown_data(mtcHost);
		} catch (IOException e) {
			final StringWriter error = new StringWriter();
			e.printStackTrace(new PrintWriter(error));
			throw new TtcnError("Sending data on the control connection to HC failed.");
		}
	}
	
	
	private static void handle_unknown_data(Host mtc) {
		Text_Buf local_incoming_buf = incoming_buf.get();
		receiveMessage(mtc);
		
		do {
			final int msg_len = local_incoming_buf.pull_int().get_int();
			final int msg_type = local_incoming_buf.pull_int().get_int();
			boolean process_more_messages = false;
			switch (msg_type) {
	        case MSG_ERROR:
	        	process_error(mtc);
	        	process_more_messages = true;
	        	break;
	        case MSG_LOG:
	        	process_log(mtc);
	        	process_more_messages = true;
	        	break;
	        case MSG_VERSION:
	        	process_version(mtc); 
	        	break;
	        case MSG_MTC_CREATED:
	        	process_mtc_created(mtc);
	        	break;
	        case MSG_PTC_CREATED:
	        	process_ptc_created(mtc);
	        	break;
	        default:
	        	// TODO error 
			}
			if (process_more_messages) {
				local_incoming_buf.cut_message();
				receiveMessage(mtc);
			}
			else {
				break;
			}
		} while(local_incoming_buf.is_message());
		// TODO 
	}

	private static void process_error(ComponentStruct tc) {
		Text_Buf text_buf = incoming_buf.get();
		String reason = text_buf.pull_string();
		text_buf.cut_message();	
		if (tc.equals(mtc)) {
			// TODO error
		}
		else {
			System.out.println(MessageFormat.format("Error message was received from PTC {0} at {1} [{2}]: {3}", 
					tc.comp_ref, tc.comp_location.hostname, tc.comp_location.address, reason));
		}
	}
	
	private static void process_error(Host hc) {
		Text_Buf text_buf = incoming_buf.get();
		String reason = text_buf.pull_string();
		text_buf.cut_message();
		// TODO error
	}

	private static void connect_ptc() {
		SocketChannel sc;
		try {
			sc = serverSocketChannel.accept();
			Host ptcHost = new Host(sc);
			ptcHost.address = sc.getLocalAddress();
			setup_host(ptcHost);
			ptc.comp_location = ptcHost;
			handle_unknown_data(ptcHost);
		} catch (IOException e) {
			final StringWriter error = new StringWriter();
			e.printStackTrace(new PrintWriter(error));
			throw new TtcnError("Sending data on the control connection to HC failed.");
		}
	}

	private static void send_create_mtc(Host host) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_CREATE_MTC);
		send_message(host, text_buf);
	}

	public static void printWelcome() {
		System.out.printf("\n"+
				"*************************************************************************\n"+
			    "* TTCN-3 Test Executor - Main Controller 2                              *\n"+
			    "* Version: %-40s                     *\n"+
			    "* Copyright (c) 2000-2019 Ericsson Telecom AB                           *\n"+
			    "* All rights reserved. This program and the accompanying materials      *\n"+
			    "* are made available under the terms of the Eclipse Public License v2.0 *\n"+
			    "* which accompanies this distribution, and is available at              *\n"+
			    "* https://www.eclipse.org/org/documents/epl-2.0/EPL-2.0.html            *\n"+
			    "*************************************************************************\n"+
			    "\n", TTCN_Runtime.PRODUCT_NUMBER);
	}
	
	
	private static void handle_hc_data(Host hc) {
		Text_Buf local_incoming_buf = incoming_buf.get();
		boolean error_flag = false;
		receiveMessage(hc);
		do {
			final int msg_len = local_incoming_buf.pull_int().get_int();
			final int msg_type = local_incoming_buf.pull_int().get_int();
			switch (msg_type) {
			case MSG_CONFIGURE_ACK:
				process_configure_ack(hc);
				break;
			case MSG_ERROR:
				process_error(hc);
				break;
			case MSG_CONFIGURE_NAK:
				process_configure_nak(hc);
				break;
			case MSG_CREATE_NAK:
				// TODO 
		        break;
			case MSG_LOG:
				process_log(hc);
				break;
			case MSG_HC_READY:
				process_hc_ready(hc);
				break; 
			case MSG_DEBUG_RETURN_VALUE:
				// TODO
		        break;
			default:
				// TODO error
				error_flag = true;
			} 
			if (error_flag) {
				break;
			}
			local_incoming_buf.cut_message(); 
			if(msg_type != MSG_CONFIGURE_ACK) { 
				receiveMessage(hc);
			}			
		} while(local_incoming_buf.is_message());
		// TODO
	
	}
	
	private static void process_configure_nak(Host hc) {
		incoming_buf.get().cut_message();;
		switch(hc.hc_state) {
		case HC_CONFIGURING:
		case HC_CONFIGURING_OVERLOADED:
			hc.hc_state = hcStateEnum.HC_IDLE;
			break;
		default:
			send_error(hc, "Unexpected message CONFIGURE_NAK was received.");
			return;
		}
		
		if (mc_state == mcStateEnum.MC_CONFIGURING || mc_state == mcStateEnum.MC_RECONFIGURING) {
			check_all_hc_configured();
		}
		else {
			System.out.println("Processing of configuration file failed on host "+hc.hostname+".");
		}
	}

	private static void process_hc_ready(Host hc) {
		switch(hc.hc_state) {
			case HC_OVERLOADED:
				hc.hc_state = hcStateEnum.HC_ACTIVE;
		    break;
			case HC_CONFIGURING_OVERLOADED:
			  hc.hc_state = hcStateEnum.HC_CONFIGURING;
		    break;
			default:
				send_error(hc, "Unexpected message HC_READY was received.");
				return;
		}
		System.out.println("Host "+hc.hostname+" is no more overloaded.");
		incoming_buf.get().cut_message();
	}
	
	public static void send_error(Host hc, String reason) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_ERROR);
		text_buf.push_string(reason);
		send_message(hc, text_buf);
	}
	

	private static void process_create_req(ComponentStruct tc) {
		if (!request_allowed(mtc, "CREATE_REQ")) return;
		
		Text_Buf text_buf = incoming_buf.get();
		
		String componentTypeModule = text_buf.pull_string();
		String componentTypeName = text_buf.pull_string();
		String componentName = text_buf.pull_string();
		String componentLocation = text_buf.pull_string();
		int isAlive = text_buf.pull_int().get_int();
		int seconds = text_buf.pull_int().get_int();
		int miliseconds = text_buf.pull_int().get_int();
		text_buf.cut_message();
		
		
		// FIXME choose location
		Host ptcLoc = choose_ptc_location(componentTypeName, componentName, componentLocation);
		if(ptcLoc == null) {
			if(!is_hc_in_state(hcStateEnum.HC_ACTIVE)) {
				send_error(tc.comp_location, "There is no active HC connection. Create operation cannot be performed.");
			}
			else {
				String compData = "component type: "+componentTypeModule+"."+componentTypeName;
				if(componentName != null) {
					compData = compData+", name: "+componentName;
				}
				if (componentLocation != null) {
					compData = compData+", location: "+componentLocation;
				}
				send_error(tc.comp_location, MessageFormat.format("No suitable host was found to create a new PTC ({0}).", compData));
			}				
			return;
		}
		
		tc.tc_state = tcStateEnum.TC_CREATE;
		
		ptc = new ComponentStruct(new Text_Buf());
		ptc.comp_ref = next_comp_ref++;
		ptc.comp_name = componentName;
		ptc.comp_location = ptcLoc;
		ptc.tc_state = tcStateEnum.TC_INITIAL;
		ptc.local_verdict = VerdictTypeEnum.NONE;
		ptc.is_alive = (isAlive == 1);
		ptc.create_requestor = tc;
		ptc.done_requestors = init_requestors(null);
		ptc.killed_requestors = init_requestors(null);
		ptc.cancel_done_sent_for = init_requestors(null);
		ptc.conn_head_list = new ArrayList<PortConnection>();
		ptc.conn_tail_list = new ArrayList<PortConnection>();

		components.put(ptc.comp_ref, ptc);
		ptcLoc.addComponent(ptc);
		
		send_create_ptc(ptcLoc, ptc.comp_ref, componentTypeModule, componentTypeName, system.comp_type, 
				componentName, isAlive, mtc.tc_fn_name, seconds, miliseconds);
		
		final Thread PTC = new Thread() {
			
			@Override
			public void run() {
				connect_ptc();
			}
			
		};

		PTC.start();
	}

	
	// FIXME 
	private static Host choose_ptc_location(String componentTypeName, String componentName, String componentLocation) {
		Host location = null;
		for (Host h : hosts) { 
			if (!h.equals(mtc.comp_location)) {
				return h;
			}
		}
		return null;
	}
	

	boolean set_has_string(Set<String> set, String str) {
		if (str == null) {
			return false;
		}
		for (String element : set) {
			int result = element.compareTo(str);
			if (result == 0) {
				return true;
			}
			else if (result > 0) {
				break;
			}
		}
		return false;
	}

	
	private static void send_create_ptc(Host host, int comp_ref, String componentTypeModule, String componentTypeName, 
			QualifiedName comp_type, String componentName, int isAlive, QualifiedName tc_fn_name, int seconds, 
			int miliseconds) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_CREATE_PTC);
		text_buf.push_int(comp_ref);
		text_buf.push_string(componentTypeModule);
		text_buf.push_string(componentTypeName);
		text_buf.push_string(comp_type.module_name);
		text_buf.push_string(comp_type.definition_name);
		text_buf.push_string(componentName);
		text_buf.push_int(isAlive);
		text_buf.push_string(mtc.tc_fn_name.module_name);
		text_buf.push_string(mtc.tc_fn_name.definition_name);
		text_buf.push_int(seconds);
		text_buf.push_int(miliseconds);
		
		send_message(host, text_buf);
	}

	private static void process_configure_ack(Host hc) {
		switch (hc.hc_state) {
		case HC_CONFIGURING:
			hc.hc_state = hcStateEnum.HC_ACTIVE;
			break;
		case HC_CONFIGURING_OVERLOADED:
			hc.hc_state = hcStateEnum.HC_OVERLOADED;
		    break;
		default:
			send_error(hc, "Unexpected message CONFIGURE_ACK was received.");
		}
		
		if (mc_state == mcStateEnum.MC_CONFIGURING || mc_state == mcStateEnum.MC_RECONFIGURING) {
		    check_all_hc_configured();
		}
		else {
			System.out.println("Host "+hc.hostname+" was configured successfully.");
		}
		incoming_buf.get().cut_message();
	}

	private static void check_all_hc_configured() {
		boolean reconf = (mc_state == mcStateEnum.MC_RECONFIGURING);
		if (is_hc_in_state(hcStateEnum.HC_CONFIGURING) ||
			      is_hc_in_state(hcStateEnum.HC_CONFIGURING_OVERLOADED)) return;
	    if (is_hc_in_state(hcStateEnum.HC_IDLE)) {
	    	mc_state = reconf ? mcStateEnum.MC_READY : mcStateEnum.MC_HC_CONNECTED;
	    	// TODO error 
	    }   else if (is_hc_in_state(hcStateEnum.HC_ACTIVE) || is_hc_in_state(hcStateEnum.HC_OVERLOADED)) {
	    	System.out.println("Configuration file was processed on all HCs.");
	    	mc_state = reconf ? mcStateEnum.MC_READY : mcStateEnum.MC_ACTIVE;
	    } else {
	    	mc_state = mcStateEnum.MC_LISTENING;
	    	//TODO error 
	    }
		
	}

	private static boolean is_hc_in_state(hcStateEnum checked_state) {
		for (int i = 0; i < hosts.size(); i++) {
			if(hosts.get(i).hc_state == checked_state) {
				return true;
			}
		}
		return false;
	}

	public static void receiveMessage(Host hc) {
		final Text_Buf local_incoming_buf = incoming_buf.get();
		final AtomicInteger buf_ptr = new AtomicInteger();
		final AtomicInteger buf_len = new AtomicInteger();
		local_incoming_buf.get_end(buf_ptr, buf_len);

		final ByteBuffer tempbuffer = ByteBuffer.allocate(1024);
		int recv_len = 0;
		try {
			recv_len = hc.socket.read(tempbuffer);
		} catch (IOException e) {
			e.printStackTrace();
			throw new TtcnError(e);
		}
		if (recv_len > 0) {
			local_incoming_buf.push_raw(recv_len, tempbuffer.array());
		}
	}
	
	public static void process_version(Host hc) {
		receiveMessage(hc);
		final Text_Buf local_incoming_buf = incoming_buf.get();
		final int msg_len = local_incoming_buf.pull_int().get_int();
		final int msg_type = local_incoming_buf.pull_int().get_int();
		
		if (msg_type == MSG_VERSION) {
			for (int i=0; i<4; i++) {
				local_incoming_buf.pull_int();
			}
			int modules_size = local_incoming_buf.pull_int().get_int();
			for(int i=0; i<modules_size; i++) {
				local_incoming_buf.pull_string();
				int value = local_incoming_buf.pull_int().get_int();
				byte[] data = new byte[16];
				if (value == 16) {
					local_incoming_buf.pull_raw(16, data);
				}
			}

			add_new_host(hc);
			local_incoming_buf.cut_message();
		}
	}
	
	private static void add_new_host(Host hc) {
		Text_Buf text_buf = incoming_buf.get();
		hc.hostname = text_buf.pull_string();
		hc.machine_type = text_buf.pull_string();
		hc.system_name = text_buf.pull_string();
		hc.system_release = text_buf.pull_string();
		hc.system_version = text_buf.pull_string();

		for (int i = 0; i < transport_type_enum.TRANSPORT_NUM.ordinal(); i++) {
		    hc.transport_supported[i] = false;
		}
		int n_supported_transports = text_buf.pull_int().get_int();
		for (int i=0; i<n_supported_transports; i++) {
			int transport_type = text_buf.pull_int().get_int();
			if (transport_type >= 0 && transport_type < transport_type_enum.TRANSPORT_NUM.ordinal()) {
				if (hc.transport_supported[transport_type]) {
					send_error(hc, MessageFormat.format("Malformed VERSION message was received: Transport type {0} "
							+ " was specified more than once.", transport_type_enum.values()[transport_type].toString()));
				}
				else {
					hc.transport_supported[transport_type] = true;
				}
			}
			else {
				send_error(hc, MessageFormat.format("Malformed VERSION message was received: Transport type code {0} "
						+ "is invalid.", transport_type));
			}
		}

		if (!hc.transport_supported[transport_type_enum.TRANSPORT_LOCAL.ordinal()]) {
			send_error(hc, MessageFormat.format("Malformed VERSION message was received: Transport type {0} "
					+ " must be supported anyway.", transport_type_enum.TRANSPORT_LOCAL.toString()));
		}
		if (!hc.transport_supported[transport_type_enum.TRANSPORT_INET_STREAM.ordinal()]) {
			send_error(hc, MessageFormat.format("Malformed VERSION message was received: Transport type {0} "
					+ " must be supported anyway.", transport_type_enum.TRANSPORT_INET_STREAM.toString()));
		}
		
		hc.hc_state = hcStateEnum.HC_IDLE;
		text_buf.cut_message();
	}
	
	private static void setup_host(Host host) {
		host.transport_supported[transport_type_enum.TRANSPORT_LOCAL.ordinal()] = true;
		host.transport_supported[transport_type_enum.TRANSPORT_INET_STREAM.ordinal()] = true;
		
	}

	private static void configure() {		
		switch(mc_state) {
		case MC_HC_CONNECTED:
		case MC_ACTIVE:
			mc_state = mcStateEnum.MC_CONFIGURING;
		    break;
		case MC_LISTENING:
		case MC_LISTENING_CONFIGURED:
			mc_state = mcStateEnum.MC_LISTENING_CONFIGURED;
		    break;
		case MC_RECONFIGURING:
		    break;
		default:
			//TODO error 
			return;
		}
		
		if (mc_state == mcStateEnum.MC_CONFIGURING || mc_state == mcStateEnum.MC_RECONFIGURING) {
			System.out.println("Downloading configuration file to all HCs.");
			for (Host host : hosts) {
		    	configure_host(host, false);		    	
		    	handle_hc_data(host);
		    }
		}
	  
		if (mc_state == mcStateEnum.MC_RECONFIGURING) {
			System.out.println("Downloading configuration file to the MTC.");
			configure_mtc();
		}
	}

	private static void configure_mtc() {
		if (config_str.get() == null) {
			//TODO error
		}
		if (mtc.tc_state == tcStateEnum.TC_IDLE) {
			//TODO error 
		}
		else {
			mtc.tc_state = tcStateEnum.MTC_CONFIGURING;
			send_configure_mtc(config_str.get());
		}
		
	}

	private static void send_configure_mtc(String config) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_CONFIGURE);
		text_buf.push_string(config);
		send_message(mtc.comp_location, text_buf);
	}

	private static void configure_host(Host host, boolean should_notify) {
		hcStateEnum next_state = hcStateEnum.HC_CONFIGURING;
		switch(host.hc_state) {
		case HC_CONFIGURING:
		case HC_CONFIGURING_OVERLOADED:
		case HC_EXITING:
			//TODO error 
			break;
		case HC_DOWN:
			break;
		case HC_OVERLOADED:
		    next_state =hcStateEnum.HC_CONFIGURING_OVERLOADED;
		    // no break
		default:
			host.hc_state = next_state;
			if (should_notify) {
				System.out.println("Downloading configuration file to HC on host "+host.hostname+".");
			}
		    send_configure(host);
		    
		    if (mc_state != mcStateEnum.MC_RECONFIGURING) {
		    	// TODO send debug setup
		    }
		}
		
	}

	private static void send_configure(Host hc) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_CONFIGURE);
		text_buf.push_string(config_str.get());
		
		send_message(hc, text_buf);
	}

	private static void send_message(Host hc, Text_Buf text_buf) {
		text_buf.calculate_length();
		final byte[] msg_ptr = text_buf.get_data();
		final int msg_len = text_buf.get_len();
		final ByteBuffer buffer = ByteBuffer.wrap(msg_ptr, text_buf.get_begin(), msg_len);
		final SocketChannel localChannel = hc.socket;
		try {
			while (buffer.hasRemaining()) {
				localChannel.write(buffer);
			}
		} catch (IOException e) {
			close_connection(hc);
			final StringWriter error = new StringWriter();
			e.printStackTrace(new PrintWriter(error));
			throw new TtcnError("Sending data on the control connection to MC failed.");
		}		
	}

	private static void process_mtc_created(Host hc) {
		if(mc_state != mcStateEnum.MC_CREATING_MTC) {
			send_error(hc, "Message MTC_CREATED arrived in invalid state.");
			return;
		}
		if (mtc == null || mtc.tc_state != tcStateEnum.TC_INITIAL ) {
			//TODO error
			return;
		}

		mc_state = mcStateEnum.MC_READY;
		mtc.tc_state = tcStateEnum.TC_IDLE;
				
		System.out.println("MTC is created.");
		for (ExecuteItem item : executeItems) {
			if (item.getTestcaseName() == null) { 
				execute_control(hc, item.getModuleName());
			} 
			else if (!"*".equals(item.getTestcaseName())) {
				execute_testcase(hc, item.getModuleName(), null);
			} 
			else {
				execute_testcase(hc, item.getModuleName(), item.getTestcaseName());
			}
			
			handle_tc_data(mtc);
		}
		
		incoming_buf.get().cut_message();
		exit_mtc();
		
		if (mc_state != mcStateEnum.MC_TERMINATING_MTC) {
			System.out.println("The control connection to MTC is lost. Destroying all PTC connections.");
		}
		// TODO destroy_all_components();
		System.out.println("MTC terminated.");
		if (is_hc_in_state(hcStateEnum.HC_CONFIGURING)) {
			mc_state = mcStateEnum.MC_CONFIGURING;
		}
		else if (is_hc_in_state(hcStateEnum.HC_IDLE)) {
			mc_state = mcStateEnum.MC_HC_CONNECTED;
		}
		else if (is_hc_in_state(hcStateEnum.HC_ACTIVE) || is_hc_in_state(hcStateEnum.HC_OVERLOADED)) {
			mc_state = mcStateEnum.MC_ACTIVE;
		}
		else {
			mc_state = mcStateEnum.MC_LISTENING_CONFIGURED;
		}
		stop_requested.set(false);
		
		shutdown_session();
		System.out.println("Shutdown complete.");
	}
	

	private static void handle_tc_data(ComponentStruct tc) {
		Text_Buf local_incoming_buf = incoming_buf.get();
		
		receiveMessage(tc.comp_location);
		boolean close_connection = false;
		do {
			final int msg_len = local_incoming_buf.pull_int().get_int();
			final int msg_end = local_incoming_buf.get_pos() + msg_len;
			final int msg_type = local_incoming_buf.pull_int().get_int();
			switch (msg_type) {
	        case MSG_ERROR: 
	        	process_error(tc);
	        	break;
	        case MSG_LOG:
	        	process_log(tc.comp_location);
	        	break;
	        case MSG_CREATE_REQ:
	        	process_create_req(tc);
	        	break;
	        case MSG_START_REQ:
	        	process_start_req(tc);
	        	break;
	        case MSG_STOP_REQ:
	        	process_stop_req(tc);
	        	break;
	        case MSG_KILL_REQ:
	        	process_kill_req(tc);
	        	break;
	        case MSG_IS_RUNNING:
	        	process_is_running(tc);
	        	break;
	        case MSG_IS_ALIVE:
	        	process_is_alive(tc);
	        	break;
	        case MSG_DONE_REQ:
	        	process_done_req(tc);
	        	break;
	        case MSG_KILLED_REQ:
	        	process_killed_req(tc);
	        	break;
	        case MSG_CANCEL_DONE_ACK:
	        	process_cancel_done_ack(tc);
	        	break;
	        case MSG_CONNECT_REQ:
	        	process_connect_req(tc);
	        	break;
	        case MSG_CONNECT_LISTEN_ACK:
	        	process_connect_listen_ack(tc, msg_end);
	        	break;
	        case MSG_CONNECTED:
	        	process_connected(tc);
	        	break;
	        case MSG_CONNECT_ERROR:
	        	process_connect_error(tc);
	        	break;
	        case MSG_DISCONNECT_REQ:
	        	process_disconnect_req(tc);
	        	break;
	        case MSG_DISCONNECTED:
	        	process_disconnected(tc);
	        	break;
	        case MSG_MAP_REQ:
	        	process_map_req(tc);
	        	break;
	        case MSG_MAPPED:
	        	process_mapped(tc);
	        	break;
	        case MSG_UNMAP_REQ:
	        	process_unmap_req(tc);
        		break;
	        case MSG_UNMAPPED:
	        	process_unmapped(tc);
	        	break;
	        case MSG_DEBUG_RETURN_VALUE:
	        	// TODO 
	        	break;
	        case MSG_DEBUG_HALT_REQ:
	        	// TODO 
	        	break;
	        case MSG_DEBUG_CONTINUE_REQ:
	        	// TODO
	        	break;
	        case MSG_DEBUG_BATCH:
	        	// TODO
	        	break;
	        default:
	        	if (tc.equals(mtc)) {
	        		// these messages can be received only from the MTC
	        		switch (msg_type) {
	        		case MSG_TESTCASE_STARTED:
	        			process_testcase_started();
	        			break;
	        		case MSG_TESTCASE_FINISHED:
	        			process_testcase_finished();
	        			break;
	        		case MSG_MTC_READY:
	        			process_mtc_ready();
	        			break;
	        		case MSG_CONFIGURE_ACK:
	        			process_configure_ack_mtc();
	        			break;
	        		case MSG_CONFIGURE_NAK:
	        			process_configure_nak_mtc();
	        			break; 
	        		default:
	        			// TODO error
	        			close_connection = true;
	        		}
	          } else {
	        	  // these messages can be received only from PTCs
	        	  switch (msg_type) {
	        	  case MSG_STOPPED:
	        		  process_stopped(tc, msg_end);
	        		  break;
	        	  case MSG_STOPPED_KILLED:
	        		  process_stopped_killed(tc, msg_end);
	        		  break;
	        	  case MSG_KILLED:
	        		  process_killed(tc);
	        		  break;
	        	  default:
	        		  System.out.println(MessageFormat.format("Invalid message type ({}) was received from PTC {1} "
	        		  		+ "at {2} [{3}].", msg_type, tc.comp_ref, tc.comp_location.hostname, 
	        				  tc.comp_location.address.toString()));
	        		  close_connection = true;
	        		  // TODO 
	        	  } 
	          	}
	        }
			
			if (close_connection) {
				break;
			}
			
			if (local_incoming_buf.get_len() == local_incoming_buf.get_pos()) {
				local_incoming_buf.cut_message();
				if (!(msg_type == MSG_MTC_READY && tc.equals(mtc))) {	
					receiveMessage(tc.comp_location);
				}
			}
		} while(local_incoming_buf.is_message());
		// FIXME
		
		if (!tc.equals(mtc)) { 
			if (tc.tc_state != tcStateEnum.TC_EXITING) {
				tc.local_verdict = VerdictTypeEnum.ERROR;
				component_terminated(tc);
			}
			tc.tc_state = tcStateEnum.TC_EXITED;
			if (mc_state == mcStateEnum.MC_TERMINATING_TESTCASE && ready_to_finish_testcase()) {
				finish_testcase();
			}
		}
	}

	private static boolean ready_to_finish_testcase() {
		for (int i=tc_first_comp_ref; i<= components.size(); i++) {			
			ComponentStruct comp = components.get(i);
			switch(comp.tc_state) {
			case TC_EXITED:
			case PTC_STALE:
				break;
			default:
				return false;
			}
		}
		return true;
	}

	private static void process_stopped(ComponentStruct tc, int msg_end) {
		switch(tc.tc_state) {
		case TC_STOPPING:
		case PTC_FUNCTION:
		case PTC_STOPPING_KILLING:
			if (tc.is_alive) {
				break;
			}
		default: 
			send_error(tc.comp_location, "Unexpected message STOPPED was received.");
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		tc.local_verdict = VerdictTypeEnum.values()[text_buf.pull_int().get_int()];
		tc.verdict_reason = text_buf.pull_string();
		tc.return_type = text_buf.pull_string();
		byte[] return_value = new byte[text_buf.get_len() - text_buf.get_pos()];
		text_buf.pull_raw(return_value.length, return_value);
		text_buf.cut_message();
		
		component_stopped(tc);
	}

	private static void component_stopped(ComponentStruct tc) {
		tcStateEnum old_state = tc.tc_state;
		if (old_state == tcStateEnum.PTC_STOPPING_KILLING) {
			tc.tc_state = tcStateEnum.PTC_KILLING;
		}
		else {
			tc.tc_state = tcStateEnum.PTC_STOPPED;
			// TODO timer
		}
		
		switch(mc_state) {
		case MC_EXECUTING_TESTCASE:
			break;
		case MC_TERMINATING_TESTCASE:
			return;
		default:
			//TODO error 
			return;
		}
		if (!tc.is_alive) {
			send_error(tc.comp_location, "Message STOPPED can only be sent by alive PTCs.");
			return;
		}
		boolean send_status_to_mtc = false;
		boolean send_done_to_mtc = false;
		for (int i=0; ; i++) {
			ComponentStruct requestor = get_requestor(tc.done_requestors, i);
			if (requestor == null) {
				break;
			}
			else if (requestor.equals(mtc)) {
				send_status_to_mtc = true;
				send_done_to_mtc = true;
			}
			else {
				send_component_status_to_requestor(tc, requestor, true, false);
			}
		}
		if (any_component_done_requested.get()) {
			send_status_to_mtc = true;
		}
		boolean all_done_checked = false;
		boolean all_done_result = false;
		if (all_component_done_requested.get()) {
			all_done_checked = true;
			all_done_result = !is_any_component_running();
			if (all_done_result) {
				send_status_to_mtc = true;
			}
		}
		
		if (send_status_to_mtc) {
			if (!all_done_checked) {
				all_done_result = !is_any_component_running();
			}
			if (send_done_to_mtc) {
				send_component_status_mtc(tc.comp_ref, true, false, any_component_done_requested.get(), all_done_result, 
						false, false, tc.local_verdict, tc.return_type, tc.return_value);
			}
			else {
				send_component_status_mtc(TitanComponent.NULL_COMPREF, false, false, any_component_done_requested.get(), 
						all_done_result, false, false, VerdictTypeEnum.NONE, null, null);
			}
			if (any_component_done_requested.get()) {
				any_component_done_requested.set(false);
				any_component_done_sent.set(true);
			}
			if (all_done_result) {
				all_component_done_requested.set(false);
			}
			
		}
		if (old_state != tcStateEnum.PTC_FUNCTION) {
			if (mtc.tc_state == tcStateEnum.MTC_ALL_COMPONENT_KILL) {
				// do nothing
			}
			else if (mtc.tc_state == tcStateEnum.MTC_ALL_COMPONENT_STOP) {
				check_all_component_stop();
			}
			else {
				send_stop_ack_to_requestors(tc);
			}
		}
	}

	private static void process_unmapped(ComponentStruct tc) {
		if (!message_expected(tc, "UNMAPPED")) {
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		boolean translation = text_buf.pull_int().get_int() == 0 ? false : true;
		String sourcePort = text_buf.pull_string();
		String systemPort = text_buf.pull_string();
		int nof_params = text_buf.pull_int().get_int();
		final Map_Params params = new Map_Params(nof_params);
		for (int i = 0; i < nof_params; i++) {
			final String par = text_buf.pull_string();
			params.set_param(i, new TitanCharString(par));
		}
		text_buf.cut_message();
		
		PortConnection conn = null;
		if (!translation) {
			conn = find_connection(tc.comp_ref, sourcePort, TitanComponent.SYSTEM_COMPREF, systemPort);
		}
		else {
			conn = find_connection(TitanComponent.SYSTEM_COMPREF, sourcePort, tc.comp_ref, systemPort);
		}
		
		if (conn != null) {
			switch(conn.conn_state) {
			case CONN_MAPPING:
			case CONN_MAPPED:
			case CONN_UNMAPPING:
				destroy_mapping(conn, nof_params, params, null);
				break;
			default:
				send_error(tc.comp_location, MessageFormat.format("Unexpected MAPPED message was received for "
						+ "port mapping {0}:{1} - system:{2}.", tc.comp_ref, sourcePort, systemPort));
			}
		}
		
	}

	private static void process_unmap_req(ComponentStruct tc) {
		if (!request_allowed(tc, "UNMAP_REQ")) {
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		int sourceComponent = text_buf.pull_int().get_int();
		boolean translation = text_buf.pull_int().get_int() == 0 ? false : true;
		String sourcePort = text_buf.pull_string();
		String systemPort = text_buf.pull_string();
		int nof_params = text_buf.pull_int().get_int();

		final Map_Params params = new Map_Params(nof_params);
		for (int i = 0; i < nof_params; i++) {
			final String par = text_buf.pull_string();
			params.set_param(i, new TitanCharString(par));
		}
		text_buf.cut_message();
		
		if (!valid_endpoint(sourceComponent, false, tc, "unmap")) {
		    return;
		}
		
		PortConnection conn = find_connection(sourceComponent, sourcePort, TitanComponent.SYSTEM_COMPREF, systemPort);
		if (conn == null) {
			send_unmap_ack(tc, nof_params, params); 
		}
		else {
		    switch (conn.conn_state) {
		    case CONN_MAPPED:
		    	send_unmap(components.get(sourceComponent), sourcePort, systemPort, nof_params, params, translation);
		    	conn.conn_state = connStateEnum.CONN_UNMAPPING;
		    case CONN_UNMAPPING:
		    	add_requestor(conn.requestors, tc);
		    	tc.tc_state = tcStateEnum.TC_UNMAP;
		    	break;
		    case CONN_MAPPING:
		    	send_error(tc.comp_location, MessageFormat.format("The port mapping {0}:{1} - system:{2} cannot be "
		    			+ "destroyed because a map operation is in progress on it.", sourceComponent, sourcePort, systemPort));
		    	break;
		    default:
		    	send_error(tc.comp_location, MessageFormat.format("The port mapping {0}:{1} - system:{2} is in invalid "
		    			+ "state.", sourceComponent, sourcePort, systemPort));
		    }
		}
	}

	private static void send_unmap(ComponentStruct tc, String sourcePort, String systemPort,
			int nof_params, Map_Params params, boolean translation) {

		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_UNMAP);
		text_buf.push_int(translation ? 1 : 0);
		text_buf.push_string(sourcePort);
		text_buf.push_string(systemPort);
		text_buf.push_int(nof_params);
		for (int i=0; i<nof_params; i++) {
			text_buf.push_string(params.get_param(i).get_value().toString());
		}
		
		send_message(tc.comp_location, text_buf);
	}

	private static void process_killed_req(ComponentStruct tc) {
		if (!request_allowed(tc, "KILLED_REQ")) {
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		int component_reference = text_buf.pull_int().get_int();
		text_buf.cut_message();

		switch (component_reference) {
		case TitanComponent.NULL_COMPREF:
		    send_error(tc.comp_location, "Killed operation was requested on the null component reference.");
		    return;
		case TitanComponent.MTC_COMPREF:
		    send_error(tc.comp_location, "Killed operation was requested on the component reference of the MTC.");
		    return;
		case TitanComponent.SYSTEM_COMPREF:
		    send_error(tc.comp_location, "Killed operation was requested on the component reference of the system.");
		    return;
		case TitanComponent.ANY_COMPREF:
		    if (tc.equals(mtc)) {
		      boolean answer = !is_all_component_alive();
		      send_killed_ack(mtc, answer);
		      if (!answer) {
		    	  any_component_killed_requested.set(true);
		      }
		    } else {
		    	send_error(tc.comp_location, "Operation 'any component.killed' can only be performed on the MTC.");
		    }
		    return;
		case TitanComponent.ALL_COMPREF:
		    if (tc == mtc) {
		      boolean answer = !is_any_component_alive();
		      send_killed_ack(mtc, answer);
		      if (!answer) {
		    	  all_component_killed_requested.set(true);
		      }
		    } else {
		    	send_error(tc.comp_location, "Operation 'all component.killed' can only be performed on the MTC.");
		    }
		    return;
		default:
		    break;
		}
		
		ComponentStruct comp = components.get(component_reference);
		if (comp == null) {
		    send_error(tc.comp_location, MessageFormat.format("The argument of killed operation is an invalid "
		    		+ "component reference: {0}.", component_reference));
		    return;
		}
		switch (comp.tc_state) {
		case TC_EXITING:
		case TC_EXITED:
		    send_killed_ack(tc, true);
		    break;
		case TC_IDLE:
		case TC_CREATE:
		case TC_START:
		case TC_STOP:
		case TC_KILL:
		case TC_CONNECT:
		case TC_DISCONNECT:
		case TC_MAP:
		case TC_UNMAP:
		case TC_STOPPING:
		case PTC_FUNCTION:
		case PTC_STARTING:
		case PTC_STOPPED:
		case PTC_KILLING:
		case PTC_STOPPING_KILLING:
		    send_killed_ack(tc, false);
		    add_requestor(comp.killed_requestors, tc);
		    break;
		case PTC_STALE:
		    send_error(tc.comp_location, MessageFormat.format("The argument of killed operation ({0}) is a component "
		    		+ "reference that belongs to an earlier testcase.", component_reference));
		    break;
		default:
		    send_error(tc.comp_location, MessageFormat.format("The test component that the killed operation refers to "
		    		+ "({0}) is in invalid state.", component_reference));
		}		
	}

	private static void send_killed_ack(ComponentStruct tc, boolean answer) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_KILLED_ACK);
		text_buf.push_int(answer ? 1 : 0);
		
		send_message(tc.comp_location, text_buf);
	}

	private static void process_done_req(ComponentStruct tc) {
		if (!request_allowed(tc, "DONE_REQ")) {
			return;
		}

		Text_Buf text_buf = incoming_buf.get();
		int component_reference = text_buf.pull_int().get_int();
		text_buf.cut_message();
		
		switch (component_reference) {
		case TitanComponent.NULL_COMPREF:
		    send_error(tc.comp_location, "Done operation was requested on the null component reference.");
		    return;
		case TitanComponent.MTC_COMPREF:
		    send_error(tc.comp_location, "Done operation was requested on the component reference of the MTC.");
		    return;
		case TitanComponent.SYSTEM_COMPREF:
		    send_error(tc.comp_location, "Done operation was requested on the component reference of the system.");
		    return;
		case TitanComponent.ANY_COMPREF:
		    if (tc.equals(mtc)) {
		      boolean answer = is_any_component_done();
		      send_done_ack(mtc, answer, VerdictTypeEnum.NONE, null, null);
		      if (answer) {
		    	  any_component_done_sent.set(true);
		      }
		      else {
		    	  any_component_done_requested.set(true);
		      }
		    } else {
		    	send_error(tc.comp_location, "Operation 'any component.done' can only be performed on the MTC.");
		    }
		    return;
		case TitanComponent.ALL_COMPREF:
		    if (tc.equals(mtc)) {
		      boolean answer = !is_any_component_running();
		      send_done_ack(mtc, answer, VerdictTypeEnum.NONE, null, null);
		      if (!answer) {
		    	  all_component_done_requested.set(true);
		      }
		    } else {
		    	send_error(tc.comp_location, "Operation 'all component.done' can only be performed on the MTC.");
		    }
		    return;
		default:
		    break;
		}
		
		ComponentStruct comp = components.get(component_reference);
		if (comp == null) {
		    send_error(tc.comp_location, MessageFormat.format("The argument of done operation is an invalid component "
		    		+ "reference: {0}.", component_reference));
		    return;
		}
		
		switch (comp.tc_state) {
		case PTC_STOPPED:
		    // this answer has to be cancelled when the component is re-started
		    add_requestor(comp.done_requestors, tc);
		    // no break
		case TC_EXITING:
		case TC_EXITED:
		case PTC_KILLING:
			send_done_ack(tc, true, comp.local_verdict, comp.return_type, comp.return_value);
		    break;
		case TC_IDLE:
		case TC_CREATE:
		case TC_START:
		case TC_STOP:
		case TC_KILL:
		case TC_CONNECT:
		case TC_DISCONNECT:
		case TC_MAP:
		case TC_UNMAP:
		case TC_STOPPING:
		case PTC_FUNCTION:
		case PTC_STARTING:
		case PTC_STOPPING_KILLING:
		    send_done_ack(tc, false, VerdictTypeEnum.NONE, null, null);
		    add_requestor(comp.done_requestors, tc);
		    break;
		case PTC_STALE:
		    send_error(tc.comp_location, MessageFormat.format("The argument of done operation ({0}) "
		    		+ "is a component reference that belongs to an earlier testcase.", component_reference));
		    break; 
		default:
		    send_error(tc.comp_location, MessageFormat.format("The test component that the done operation refers to ({0}) "
		    		+ "is in invalid state.", component_reference));
		}
	}

	private static void send_done_ack(ComponentStruct tc, boolean answer, VerdictTypeEnum verdict, String return_type, byte[] return_value) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_DONE_ACK);
		text_buf.push_int(answer ? 1 : 0);
		text_buf.push_int(verdict.getValue());
		text_buf.push_string(return_type);
		if (return_value != null) {
			text_buf.push_raw(return_value.length, return_value);
		}
		send_message(tc.comp_location, text_buf);		
	}

	private static void process_cancel_done_ack(ComponentStruct tc) {
		Text_Buf text_buf = incoming_buf.get();
		int component_reference = text_buf.pull_int().get_int();
		text_buf.cut_message();
		switch (component_reference) {
		case TitanComponent.NULL_COMPREF:
		    send_error(tc.comp_location, "Message CANCEL_DONE_ACK refers to the null component reference.");
		    return;
		case TitanComponent.MTC_COMPREF:
		    send_error(tc.comp_location, "Message CANCEL_DONE_ACK refers to the component reference of the MTC.");
		    return;
		case TitanComponent.SYSTEM_COMPREF:
		    send_error(tc.comp_location, "Message CANCEL_DONE_ACK refers to the component reference of the system.");
		    return;
		case TitanComponent.ANY_COMPREF:
		    send_error(tc.comp_location, "Message CANCEL_DONE_ACK refers to 'any component'.");
		    return;
		case TitanComponent.ALL_COMPREF:
		    send_error(tc.comp_location, "Message CANCEL_DONE_ACK refers to 'all component'.");
		    return;
		default:
		    break;
		}
		ComponentStruct started_tc = components.get(component_reference);
		
		if (started_tc == null) {
		    send_error(tc.comp_location, MessageFormat.format("Message CANCEL_DONE_ACK refers to an invalid "
		    		+ "component reference: {0}.", component_reference));
		    return;
		}
		done_cancelled(tc, started_tc);
		remove_requestor(tc.cancel_done_sent_for, started_tc);
	}

	private static void process_is_alive(ComponentStruct tc) {
		if (!request_allowed(tc, "IS_ALIVE")) {
			return;
		}

		Text_Buf text_buf = incoming_buf.get();
		int component_reference = text_buf.pull_int().get_int();
		text_buf.cut_message();
		switch (component_reference) {
		case TitanComponent.NULL_COMPREF:
			send_error(tc.comp_location, "Alive operation was requested on the null component reference.");
		    return;
		case TitanComponent.MTC_COMPREF:
		    send_error(tc.comp_location, "Alive operation was requested on the component reference of the MTC.");
		    return;
		case TitanComponent.SYSTEM_COMPREF:
		    send_error(tc.comp_location, "Alive operation was requested on the component reference of the system.");
		    return;
		case TitanComponent.ANY_COMPREF:
		    if (tc.equals(mtc)) {
		    	send_alive(mtc, is_any_component_alive());
		    }
		    else {
		    	send_error(tc.comp_location, "Operation 'any component.alive' can only be performed on the MTC.");
		    }
		    return;
		case TitanComponent.ALL_COMPREF:
		    if (tc.equals(mtc)) {
		    	send_alive(mtc, is_all_component_alive());
		    }
		    else { 
		    	send_error(tc.comp_location, "Operation 'all component.alive' can only be performed on the MTC.");
		    }
		    return;
		default:
		    break;
		}
		
		ComponentStruct comp = components.get(component_reference);
		if (comp == null) {
		    send_error(tc.comp_location, MessageFormat.format("The argument of alive operation is an invalid "
		    		+ "component reference: {0}.", component_reference));
		    return;
		}
		
		switch (comp.tc_state) {
		case TC_IDLE:
		case TC_CREATE:
		case TC_START:
		case TC_STOP:
		case TC_KILL:
		case TC_CONNECT:
		case TC_DISCONNECT:
		case TC_MAP:
		case TC_UNMAP:
		case TC_STOPPING:
		case PTC_FUNCTION:
		case PTC_STARTING:
		case PTC_STOPPED:
		case PTC_KILLING:
		case PTC_STOPPING_KILLING:
		    send_alive(tc, true);
		    break;
		case TC_EXITING:
		case TC_EXITED:
		    send_alive(tc, false);
		    break;
		case PTC_STALE:
		    send_error(tc.comp_location, MessageFormat.format("The argument of alive operation ({0}) is a "
		    		+ "component reference that belongs to an earlier testcase.", component_reference));
		    break;
		default:
			send_error(tc.comp_location, MessageFormat.format("The test component that the alive operation referes to "
					+ "({0}) is in invalid state.", component_reference));
		}
	}

	private static boolean is_all_component_alive() {
		for (int i=tc_first_comp_ref; i<=components.size(); i++) {
			ComponentStruct comp = components.get(i);
			if (!comp.is_alive) {
				return false;
			}
		}
		return true;
	}

	private static void send_alive(ComponentStruct tc, boolean answer) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_ALIVE);
		text_buf.push_int(answer ? 1 : 0);
		send_message(tc.comp_location, text_buf);
	}

	private static void process_is_running(ComponentStruct tc) {
		if (!request_allowed(tc, "IS_RUNNING")) {
			return;
		}
		Text_Buf text_buf = incoming_buf.get();
		int component_reference = text_buf.pull_int().get_int();
		text_buf.cut_message();
		switch (component_reference) {
		case TitanComponent.NULL_COMPREF:
		    send_error(tc.comp_location, "Running operation was requested on the null component reference.");
		    return;
		case TitanComponent.MTC_COMPREF:
		    send_error(tc.comp_location, "Running operation was requested on the component reference of the MTC.");
		    return;
		case TitanComponent.SYSTEM_COMPREF:
		    send_error(tc.comp_location, "Running operation was requested on the component reference of the system.");
		    return;
		case TitanComponent.ANY_COMPREF:
		    if (tc.equals(mtc)) {
		    	send_running(mtc, is_any_component_running());
		    }
		    else {
		    	send_error(tc.comp_location, "Operation 'any component.running' can only be performed on the MTC.");
		    }
		    return;
		case TitanComponent.ALL_COMPREF:
		    if (tc.equals(mtc)) {
		    	send_running(mtc, is_all_component_running());
		    }
		    else {
		    	send_error(tc.comp_location, "Operation 'all component.running' can only be performed on the MTC.");
		    }
		    return;
		default:
		    break;
		}
		
		ComponentStruct comp = components.get(component_reference);
		if (comp == null) {
			send_error(tc.comp_location, MessageFormat.format("The argument of running operation is an invalid "
					+ "component reference: {0}.", component_reference));
			return;
		}
		
		switch (comp.tc_state) {
		case TC_CREATE:
		case TC_START:
		case TC_STOP:
		case TC_KILL:
		case TC_CONNECT:
		case TC_DISCONNECT:
		case TC_MAP:
		case TC_UNMAP:
		case TC_STOPPING:
		case PTC_FUNCTION:
		case PTC_STARTING:
		case PTC_STOPPING_KILLING:
		    send_running(tc, true);
		    break;
		case TC_IDLE:
		case TC_EXITING:
		case TC_EXITED:
		case PTC_STOPPED:
		case PTC_KILLING:
		    send_running(tc, false);
		    break;
		case PTC_STALE:
			send_error(tc.comp_location, MessageFormat.format("The argument of running operation ({0}) "
					+ "is a component reference that belongs to an earlier testcase.", component_reference));
		    break;
		default:
			send_error(tc.comp_location, MessageFormat.format("The test component that the running operation refers "
					+ "to ({0}) is in invalid state.", component_reference));
		}
	}

	private static boolean is_all_component_running() {
		for (int i=tc_first_comp_ref; i<=components.size(); i++) {
			ComponentStruct tc = components.get(i);
			if (tc.stop_requested) {
				continue;
			}
			switch(tc.tc_state) {
			case TC_EXITING:
			case TC_EXITED:
			case PTC_STOPPED:
				return false;
			default: 
				break;
			}
		}
		return true;
	}

	private static void send_running(ComponentStruct tc, boolean answer) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_RUNNING);
		text_buf.push_int(answer ? 1 : 0);
		send_message(tc.comp_location, text_buf);
	}

	private static void process_mapped(ComponentStruct tc) {
		if (!message_expected(tc, "MAPPED")) {
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		boolean translation = text_buf.pull_int().get_int() == 0 ? false : true;
		String localPort = text_buf.pull_string();
		String systemPort = text_buf.pull_string();
		int nof_params = text_buf.pull_int().get_int();

		final Map_Params params = new Map_Params(nof_params);
		for (int i = 0; i < nof_params; i++) {
			final String par = text_buf.pull_string();
			params.set_param(i, new TitanCharString(par));
		}
		
		text_buf.cut_message();
		PortConnection conn = null;
		if (!translation) {
			conn = find_connection(tc.comp_ref, localPort, TitanComponent.SYSTEM_COMPREF, systemPort);
		}
		else {
			conn = find_connection(TitanComponent.SYSTEM_COMPREF, localPort, tc.comp_ref, systemPort);
		}
		if (conn == null) {
			send_error(tc.comp_location, MessageFormat.format("The MAPPED message refers to a non-existent "
					+ "port mapping {0}:{1} - system:{2}.", tc.comp_ref, localPort, systemPort));
		}
		else if (conn.conn_state != connStateEnum.CONN_MAPPING && conn.conn_state != connStateEnum.CONN_MAPPED
				&& translation) {
			send_error(tc.comp_location, MessageFormat.format("Unexpected MAPPED message was received for mapping "
					+ "{0}:{1} - system:{2}.", tc.comp_ref, localPort, systemPort));
		}
		else {
			for (int i=0; ; i++) {
				ComponentStruct comp = get_requestor(conn.requestors, i);
				if (comp == null) {
					break;
				}
				if (comp.tc_state == tcStateEnum.TC_MAP) {
					send_map_ack(comp, nof_params, params);
					if (comp.equals(mtc)) {
						comp.tc_state = tcStateEnum.MTC_TESTCASE;
					}
					else {
						comp.tc_state = tcStateEnum.PTC_FUNCTION;
					}
				}
			}
			conn.conn_state = connStateEnum.CONN_MAPPED;
		}
	}

	private static void process_killed(ComponentStruct tc) {
		System.out.println("Process killed: "+tc.tc_state.toString());
		switch(tc.tc_state) {
		case TC_IDLE:
		case PTC_STOPPED:
		case PTC_KILLING:
			break;
		default:
			send_error(tc.comp_location, "Unexpected message KILLED was received.");
			System.out.println("Unexpected message KILLED was received from PTC "+tc.comp_ref+".");
			incoming_buf.get().cut_message();
			return;
		}
		Text_Buf text_buf = incoming_buf.get();
		tc.local_verdict = VerdictTypeEnum.values()[text_buf.pull_int().get_int()];
		tc.verdict_reason = text_buf.pull_string();
		text_buf.cut_message();
		
		if (tc.tc_state != tcStateEnum.PTC_KILLING) {
			// TODO timer
		}
		component_terminated(tc);
	}

	private static void process_configure_nak_mtc() {
		if (mtc.tc_state != tcStateEnum.MTC_CONFIGURING) {
			send_error(mtc.comp_location, "Unexpected message CONFIGURE_NAK was received.");
			return;
		}
		mtc.tc_state = tcStateEnum.TC_IDLE;
		System.out.println("Processing of configuration file failed on the MTC.");		
	}

	private static void process_configure_ack_mtc() {
		if (mtc.tc_state != tcStateEnum.MTC_CONFIGURING) {
			send_error(mtc.comp_location, "Unexpected message CONFIGURE_ACK was received.");
			return;
		}
		mtc.tc_state = tcStateEnum.TC_IDLE;
		System.out.println("Configuration file was processed on the MTC.");
	}

	private static void process_map_req(ComponentStruct tc) {
		if (!request_allowed(tc, "MAP_REQ")) {
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		int sourceComponent = text_buf.pull_int().get_int();
		boolean translation = text_buf.pull_int().get_int() == 0 ? false : true;
		String sourcePort = text_buf.pull_string();
		String systemPort = text_buf.pull_string();
		int nof_params = text_buf.pull_int().get_int();
		
		if (!valid_endpoint(sourceComponent, true, tc, "map")) {
			text_buf.cut_message();
		    return;
		}
		
		final Map_Params params = new Map_Params(nof_params);
		for (int i = 0; i < nof_params; i++) {
			final String par = text_buf.pull_string();
			params.set_param(i, new TitanCharString(par));
		}
		
		text_buf.cut_message();
		
		PortConnection conn = find_connection(sourceComponent, sourcePort, TitanComponent.SYSTEM_COMPREF, systemPort);
		if (conn == null) {
			send_map(components.get(sourceComponent), sourcePort, systemPort, nof_params, params, translation);
			conn = new PortConnection();
			conn.headComp = sourceComponent;
			conn.headPort = sourcePort;
			conn.tailComp = TitanComponent.SYSTEM_COMPREF;
			conn.tailPort = systemPort;
			conn.requestors = init_requestors(tc);
			add_connection(conn);
			tc.tc_state = tcStateEnum.TC_MAP;
		}
		else {
			switch(conn.conn_state) {
			case CONN_MAPPING:
				add_requestor(conn.requestors, tc);
				tc.tc_state = tcStateEnum.TC_MAP;
				break;
			case CONN_MAPPED:
				send_map_ack(tc, nof_params, params);
				break;
			case CONN_UNMAPPING:
				send_error(tc.comp_location, MessageFormat.format("The port mapping {0}:{1} - system:{2} cannot be "
						+ "established because an unmap operation is in progress on it.", sourceComponent, sourcePort, systemPort));
				break;
			default:
				send_error(tc.comp_location, MessageFormat.format("The port mapping {0}:{1} - system:{2} "
						+ "is in invalid state.", sourceComponent, sourcePort, systemPort));
			}
		}
	}

	private static void send_map_ack(ComponentStruct tc, int nof_params, Map_Params params) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_MAP_ACK);
		text_buf.push_int(nof_params);
		for (int i=0; i<nof_params; i++) {
			text_buf.push_string(params.get_param(i).get_value().toString());
		}
		
		send_message(tc.comp_location, text_buf);
	}

	private static void send_map(ComponentStruct tc, String sourcePort, String systemPort, int nof_params,
			Map_Params params, boolean translation) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_MAP);
		text_buf.push_int(translation ? 1 : 0);
		text_buf.push_string(sourcePort);
		text_buf.push_string(systemPort);
		text_buf.push_int(nof_params);
		for (int i=0; i<nof_params; i++) {
			text_buf.push_string(params.get_param(i).get_value().toString());
		}
		
		send_message(tc.comp_location, text_buf);
	}

	private static void process_kill_req(ComponentStruct tc) {
		if (!request_allowed(tc, "KILL_REQ")) {
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		int component_reference = text_buf.pull_int().get_int();
		text_buf.cut_message();
		switch(component_reference) {
		case TitanComponent.NULL_COMPREF:
			send_error(tc.comp_location, "Kill operation was requested on the null component reference.");
			return;
		case TitanComponent.MTC_COMPREF:
			send_error(tc.comp_location, "Kill operation was requested on the component reference of the MTC.");
			return;
		case TitanComponent.SYSTEM_COMPREF:
			send_error(tc.comp_location, "Kill operation was requested on the component reference of the system.");
			return;
		case TitanComponent.ANY_COMPREF:
			send_error(tc.comp_location, "Kill operation was requested on 'any component'.");
			return;
		case TitanComponent.ALL_COMPREF:
			if (tc.equals(mtc)) {
				if (kill_all_components(false)) {
					send_kill_ack(mtc);
				}
				else {
					mtc.tc_state = tcStateEnum.MTC_ALL_COMPONENT_KILL;
				}
			}
			else {
				send_error(tc.comp_location, "Operation 'all component.kill' can only be performed on the MTC.");
			}
			return;
		default:
			break;
		}
		
		ComponentStruct target = components.get(component_reference);
		if (target == null) {
			send_error(tc.comp_location, MessageFormat.format("The argument of kill operation is an invalid "
					+ "component reference: {0}.", component_reference));
			return;
		}
		else if (target.equals(tc)) {
			send_error(tc.comp_location, "Kill operation was requested on the requestor component itself.");
			return;
		}
		
		boolean target_inactive = false;
		switch (target.tc_state) {
		case PTC_STOPPED:
		    // the done status of this PTC is already sent out
		    // and it will not be cancelled in the future
			target.done_requestors = new RequestorStruct();
		    // no break
		case TC_IDLE:
		    target_inactive = true;
		    // no break
		case TC_CREATE:
		case TC_START:
		case TC_STOP:
		case TC_KILL:
		case TC_CONNECT:
		case TC_DISCONNECT:
		case TC_MAP:
		case TC_UNMAP:
		case PTC_FUNCTION:
		    send_kill(target);
		    if (target_inactive) {
		    	// the PTC was inactive
		    	target.tc_state = tcStateEnum.PTC_KILLING;
		    	if (!target.is_alive) {
		    		target.stop_requested = true;
		    	}
		    } else {
		    	// the PTC was active
		    	target.tc_state = tcStateEnum.PTC_STOPPING_KILLING;
		    	target.stop_requested = true;
		    }
		    target.stop_requestors = init_requestors(null);
		    target.kill_requestors = init_requestors(tc);
		    // TODO timer
		    tc.tc_state = tcStateEnum.TC_KILL;
		    break;
		case TC_STOPPING:
		    // the PTC is currently being stopped
		    send_kill(target);
		    target.tc_state = tcStateEnum.PTC_STOPPING_KILLING;
		    // TODO timer
		    // no break
		case PTC_KILLING:
		case PTC_STOPPING_KILLING:
		    // the PTC is currently being terminated
		    add_requestor(target.kill_requestors, tc);
		    tc.tc_state = tcStateEnum.TC_KILL;
		    break;
		case TC_EXITING:
		case TC_EXITED:
		    // the PTC is already terminated
		    send_kill_ack(tc);
		    break;
		case PTC_STARTING:
			send_error(tc.comp_location, MessageFormat.format("PTC with component reference {0} cannot be killed "
					+ "because it is currently being started.", component_reference));
		    break;
		case PTC_STALE:
			send_error(tc.comp_location, MessageFormat.format("The argument of kill operation ({0}) is a component "
					+ "reference that belongs to an earlier testcase.", component_reference));
		    break;
		default:
			send_error(tc.comp_location, MessageFormat.format("The test component that the kill operation refers to "
					+ "({0}) is in invalid state.", component_reference));
		}
	}

	private static void process_stopped_killed(ComponentStruct tc, int msg_end) {
		switch (tc.tc_state) {
		case TC_CREATE:
		case TC_START:
		case TC_STOP:
		case TC_KILL:
		case TC_CONNECT:
		case TC_DISCONNECT:
		case TC_MAP:
		case TC_UNMAP:
		case TC_STOPPING:
		case PTC_FUNCTION:
		case PTC_STOPPING_KILLING:
		    break;
		default:
			send_error(tc.comp_location, "Unexpected message STOPPED_KILLED was received.");
			System.out.println("Unexpected message STOPPED_KILLED was received from PTC "+tc.comp_ref+".");
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		tc.local_verdict = TitanVerdictType.VerdictTypeEnum.values()[text_buf.pull_int().get_int()];
		tc.verdict_reason = text_buf.pull_string();
		tc.return_type = text_buf.pull_string();
		text_buf.cut_message();
		
		if (tc.tc_state != tcStateEnum.PTC_STOPPING_KILLING) {
			// TODO timer
		}
		component_terminated(tc);
		
	}

	private static void component_terminated(ComponentStruct tc) {
		tcStateEnum old_state = tc.tc_state;
		tc.tc_state = tcStateEnum.TC_EXITING;
		switch(mc_state) {
		case MC_EXECUTING_TESTCASE:
			break;
		case MC_TERMINATING_TESTCASE:
			return;
		default:
			// TODO error 
			return;
		}
		
		boolean send_status_to_mtc = false;
		boolean send_done_to_mtc = true;
		for (int i=0; ; i++) {
			ComponentStruct requestor = get_requestor(tc.done_requestors, i);
			if (requestor == null) {
				break;
			}
			else if (requestor.equals(mtc)) {
				send_status_to_mtc = true;
				send_done_to_mtc = true;
			}
			else {
				send_component_status_to_requestor(tc, requestor, true, true);
			}
		}
		for (int i=0; ; i++) {
			ComponentStruct requestor = get_requestor(tc.killed_requestors, i);
			if (requestor == null) {
				break;
			}
			else if (requestor.equals(mtc)) {
				send_status_to_mtc = true;
			}
			else if (!has_requestor(tc.done_requestors, requestor)) {
				send_component_status_to_requestor(tc, requestor, false, true);
			}
		}

		tc.done_requestors = new RequestorStruct();
		tc.killed_requestors = new RequestorStruct();
		
		if (any_component_done_requested.get() || any_component_killed_requested.get()) {
			send_status_to_mtc = true;
		}
		boolean all_done_checked = false;
		boolean all_done_result = false;
		if (all_component_done_requested.get()) {
			all_done_checked = true;
			all_done_result = !is_any_component_running();
			if (all_done_result) {
				send_status_to_mtc = true;
			}
		}
		boolean all_killed_checked = false;
		boolean all_killed_result = false;
		if (all_component_killed_requested.get()) {
			all_killed_checked = true;
			all_killed_result = !is_any_component_alive();
			if (all_killed_result) {
				send_status_to_mtc = true;
			}
		}
		
		if (send_status_to_mtc) {
			if (!all_done_checked) {
				all_done_result = !is_any_component_running();
			}
			if (!all_killed_checked) {
				all_killed_result = !is_any_component_alive();
			}
			if (send_done_to_mtc) {
				send_component_status_mtc(tc.comp_ref, true, true, true, all_done_result, true, all_killed_result, 
						tc.local_verdict, tc.return_type, tc.return_value);
			}
			else {
				send_component_status_mtc(tc.comp_ref, false, true, true, all_done_result, true, all_killed_result, 
						VerdictTypeEnum.NONE, null, null);
			}
			any_component_done_requested.set(false);
			any_component_done_sent.set(true);
			any_component_killed_requested.set(false);
			if (all_done_result) {
				all_component_done_requested.set(false);
			}
			if (all_killed_result) {
				all_component_killed_requested.set(false);
			}
		}
		switch(old_state) {
		case TC_STOPPING:
		case PTC_STOPPING_KILLING:
		case PTC_KILLING:
			if (mtc.tc_state == tcStateEnum.MTC_ALL_COMPONENT_KILL) {
				check_all_component_kill();
			}
			else if (mtc.tc_state == tcStateEnum.MTC_ALL_COMPONENT_STOP) {
				check_all_component_stop();
			}
			else {
				send_stop_ack_to_requestors(tc);
				send_kill_ack_to_requestors(tc);
			}
			default:
				break;
		}
		for (int i=0; ; i++) {
			ComponentStruct comp = get_requestor(tc.cancel_done_sent_for, i);
			if (comp == null) {
				break;
			}
			done_cancelled(tc, comp);
		}
		tc.cancel_done_sent_for = new RequestorStruct();
		
		Iterator<PortConnection> it = tc.conn_head_list.iterator();
		while(it.hasNext()) {
			PortConnection conn = it.next();
			if (conn.tailComp == TitanComponent.SYSTEM_COMPREF) {
				destroy_mapping(conn, 0, null, it);
			}
			else {
				destroy_connection(conn, tc, it);
			}
			tc.conn_tail_list.remove(conn);
		}
		it = tc.conn_tail_list.iterator();
		while (it.hasNext()) {
			PortConnection conn = it.next();
			if (conn.headComp == TitanComponent.SYSTEM_COMPREF) {
				destroy_mapping(conn, 0 , null, it);
			}
			else {
				destroy_connection(conn, tc, it);
			}
			tc.conn_head_list.remove(conn);
		}
		
		tc.tc_fn_name = new QualifiedName("", "");
		
	}

	private static void destroy_connection(PortConnection conn, ComponentStruct tc, Iterator<PortConnection> it) {
		switch (conn.conn_state) {
		case CONN_LISTENING:
		case CONN_CONNECTING:
		    if (conn.transport_type != transport_type_enum.TRANSPORT_LOCAL &&
		      conn.headComp != tc.comp_ref) {
		      // shut down the server side if the client has terminated
		      send_disconnect_to_server(conn);
		    }
		    send_error_to_connect_requestors(conn, "test component "+tc.comp_ref+" has terminated during connection setup.");
		    
		    break;
		case CONN_CONNECTED:
		    break;
		case CONN_DISCONNECTING:
		    send_disconnect_ack_to_requestors(conn);
		    break;
		default:
			// TODO error 
		}
		it.remove();
	}

	private static void destroy_mapping(PortConnection conn, int nof_params, Map_Params params, Iterator<PortConnection> it) {
		int tc_compref;
		String tc_port;
		String system_port;
		if (conn.comp_ref == TitanComponent.SYSTEM_COMPREF) {
			tc_compref = conn.tailComp;
			tc_port = conn.tailPort;
			system_port = conn.headPort;
		}
		else {
			tc_compref = conn.headComp;
			tc_port = conn.headPort;
			system_port = conn.tailPort;
		}
		switch(conn.conn_state) {
		case CONN_UNMAPPING:
			for (int i=0; ; i++) {
				ComponentStruct comp = get_requestor(conn.requestors, i);
				if (comp == null) {
					break;
				}
				if (comp.tc_state == tcStateEnum.TC_UNMAP) {
					send_unmap_ack(comp, nof_params, params);
					if (comp.equals(mtc)) {
						comp.tc_state = tcStateEnum.MTC_TESTCASE;
					}
					else {
						comp.tc_state = tcStateEnum.PTC_FUNCTION;
					}
				}
			}
			break;
		case CONN_MAPPING:
			for (int i=0; ; i++) {
				ComponentStruct comp = get_requestor(conn.requestors, i);
				if (comp == null) {
					break;
				}
				if (comp.tc_state == tcStateEnum.TC_MAP) {
					send_error(comp.comp_location, MessageFormat.format("Establishment of port mapping {0}:{1} - "
							+ "system:{2} failed because the test component endpoint has terminated.", tc_compref, tc_port, system_port));
					if (comp.equals(mtc)) {
						comp.tc_state = tcStateEnum.MTC_TESTCASE;
					}
					else {
						comp.tc_state = tcStateEnum.PTC_FUNCTION;
					}
				}
			}
		default:
			break;
		}
		if (it != null) {
			it.remove();
		}
		else {
			remove_connection(conn);
		}
	}

	private static void send_unmap_ack(ComponentStruct tc, int nof_params, Map_Params params) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_UNMAP_ACK);
		text_buf.push_int(nof_params);
		for (int i=0; i<nof_params; i++) {
			text_buf.push_string(params.get_param(i).get_value().toString());
		}
		send_message(tc.comp_location, text_buf);

	}

	private static void done_cancelled(ComponentStruct from, ComponentStruct started_tc) {
		if (started_tc.tc_state != tcStateEnum.PTC_STARTING) {
			return;
		}
		remove_requestor(started_tc.cancel_done_sent_to, from);
		if (get_requestor(started_tc.cancel_done_sent_to, 0) != null) {
			return;
		}
		
		send_start(started_tc, started_tc.tc_fn_name, started_tc.arg);
		ComponentStruct start_requestor = started_tc.start_requestor;
		if (start_requestor.tc_state == tcStateEnum.TC_START) {
			send_start_ack(start_requestor);
			if(start_requestor.equals(mtc)) {
				start_requestor.tc_state = tcStateEnum.MTC_TESTCASE;
			}
			else {
				start_requestor.tc_state = tcStateEnum.PTC_FUNCTION;
			}
		}

		started_tc.cancel_done_sent_to = new RequestorStruct();
		started_tc.tc_state = tcStateEnum.PTC_FUNCTION;
	}

	private static void remove_requestor(RequestorStruct reqs, ComponentStruct tc) {
		switch (reqs.n_components) {
		case 0:
		    break;
		case 1:
			if (reqs.comp.equals(tc)) {
		    	reqs.n_components = 0;
		    }
		    break;
		case 2: 
		    ComponentStruct tmp = null;
		    if (reqs.components.get(0).equals(tc)) {
		    	tmp = reqs.components.get(1);
		    }
		    else if (reqs.components.get(1).equals(tc)) {
		    	tmp = reqs.components.get(0);
		    }
		    if (tmp != null) {
   		        reqs.n_components = 1;
		        reqs.comp = tmp;
		    }
		    break; 
		default:
			for (ComponentStruct comp : reqs.components) {
				if (comp.equals(tc)) {
			        reqs.n_components--;
			        break;
				}
			}
		}
	}

	private static void send_kill_ack_to_requestors(ComponentStruct tc) {
		for (int i = 0; ; i++) {
			ComponentStruct requestor = get_requestor(tc.kill_requestors, i);
		    if (requestor == null) {
		    	break;
		    }
		    if (requestor.tc_state == tcStateEnum.TC_KILL) {
		    	send_kill_ack(requestor);
		    	if (requestor.equals(mtc)) {
		    		requestor.tc_state = tcStateEnum.MTC_TESTCASE;
		    	}
		    	else {
		    		requestor.tc_state = tcStateEnum.PTC_FUNCTION;
		    	}
		    }
		}
		tc.kill_requestors = new RequestorStruct();
	}

	private static void send_stop_ack_to_requestors(ComponentStruct tc) {
		for (int i = 0; ; i++) {
			ComponentStruct requestor = get_requestor(tc.stop_requestors, i);
		    if (requestor == null) {
		    	break;
		    }
		    if (requestor.tc_state == tcStateEnum.TC_STOP) {
		    	send_stop_ack(requestor);
		    	if (requestor.equals(mtc)) {
		    		requestor.tc_state = tcStateEnum.MTC_TESTCASE;
		    	}
			    else {
			    	requestor.tc_state = tcStateEnum.PTC_FUNCTION;
			    }
		    }
		}		
		tc.stop_requestors = new RequestorStruct();
	}

	private static void check_all_component_stop() {
		boolean ready_for_ack = true;
		for (int i = tc_first_comp_ref; i<=components.size(); i++) {
			ComponentStruct comp = components.get(i);

		    switch (comp.tc_state) {
		    case TC_INITIAL:
		    case PTC_KILLING:
		    	if (!comp.is_alive) {
		    		ready_for_ack = false;
		    	}
		    	break;
		    case TC_STOPPING:
		    case PTC_STOPPING_KILLING:
		    	ready_for_ack = false;
		    	break;
		    case TC_EXITING:
		    case TC_EXITED:
		    case PTC_STOPPED:
		    case PTC_STALE:
		    	break;
		    case TC_IDLE:
		      // only alive components can be in idle state
		    	if (comp.is_alive) {
		    		break;
		    	}
		    default:
		    	//TODO error 
		    }
		    if (!ready_for_ack) {
		    	break;
		    }
		  }
		  if (ready_for_ack) {
			  send_stop_ack(mtc);
			  mtc.tc_state = tcStateEnum.MTC_TESTCASE;
		  }
	}

	private static void check_all_component_kill() {
		boolean ready_for_ack = true;
		for (int i = tc_first_comp_ref; i<=components.size(); i++) {
			ComponentStruct comp = components.get(i);
		    switch (comp.tc_state) {
		    case TC_INITIAL:
		    case PTC_STOPPING_KILLING:
		    case PTC_KILLING:
		    	ready_for_ack = false;
		    case TC_EXITING:
		    case TC_EXITED:
		    case PTC_STALE:
		      break;
		    default:
		    	// TODO error
		    }
		    if (!ready_for_ack) break;
		}
		if (ready_for_ack) {
		    send_kill_ack(mtc);
		    mtc.tc_state = tcStateEnum.MTC_TESTCASE;
		}
	}

	private static void send_kill_ack(ComponentStruct tc) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_KILL_ACK);
		send_message(tc.comp_location, text_buf);
	}

	private static void send_component_status_mtc(int comp_ref, boolean is_done, boolean is_killed, boolean is_any_done,
			boolean is_all_done, boolean is_any_killed, boolean is_all_killed, VerdictTypeEnum local_verdict,
			String return_type, byte[] return_value) {
		
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_COMPONENT_STATUS);
		text_buf.push_int(comp_ref);
		text_buf.push_int(is_done ? 1 : 0);
		text_buf.push_int(is_killed ? 1 : 0);
		text_buf.push_int(is_any_done ? 1 : 0);
		text_buf.push_int(is_all_done ? 1 : 0);
		text_buf.push_int(is_any_killed ? 1 : 0);
		text_buf.push_int(is_all_killed ? 1 : 0);
		text_buf.push_int(local_verdict.getValue());
		text_buf.push_string(return_type);

		if (return_value != null) {
			text_buf.push_raw(return_value);
		}
		
		send_message(mtc.comp_location, text_buf);
	}

	private static boolean is_any_component_alive() {
		for (int i = tc_first_comp_ref; i<=components.size(); i++) {
			ComponentStruct comp = components.get(i);
			if (component_is_alive(comp)) {
				return true;
			}
		}
		return false;
	}

	private static boolean component_is_alive(ComponentStruct tc) {
		switch (tc.tc_state) {
		case TC_INITIAL:
		case TC_IDLE:
		case TC_CREATE:
		case TC_START:
		case TC_STOP:
		case TC_KILL:
		case TC_CONNECT:
		case TC_DISCONNECT:
		case TC_MAP:
		case TC_UNMAP:
		case TC_STOPPING:
		case PTC_FUNCTION:
		case PTC_STARTING:
		case PTC_STOPPED:
		case PTC_KILLING:
		case PTC_STOPPING_KILLING:
			return true;
		case TC_EXITING:
		case TC_EXITED:
		    return false;
		default:
			// TODO error
		    return false;
		}
	}

	private static boolean is_any_component_running() {
		for (int i = tc_first_comp_ref; i<=components.size(); i++) {
			ComponentStruct comp = components.get(i);
			if (component_is_running(comp)) {
				return true;
			}
		}
		return false;
	}

	private static boolean component_is_running(ComponentStruct tc) {
		switch (tc.tc_state) {
		case TC_CREATE:
		case TC_START:
		case TC_STOP:
		case TC_KILL:
		case TC_CONNECT:
		case TC_DISCONNECT:
		case TC_MAP:
		case TC_UNMAP:
		case TC_STOPPING:
		case PTC_FUNCTION:
		case PTC_STARTING:
		case PTC_STOPPING_KILLING:
		    return true;
		case TC_INITIAL:
		case TC_IDLE:
		case TC_EXITING:
		case TC_EXITED:
		case PTC_STOPPED:
		case PTC_KILLING:
		    return false;
		default:
			// TODO error
		    return false;
		}
	}

	private static void send_component_status_to_requestor(ComponentStruct tc, ComponentStruct requestor, boolean done_status,
			boolean killed_status) {
		switch (requestor.tc_state) {
		case PTC_FUNCTION:
		case TC_CREATE:
		case TC_START:
		case TC_STOP:
		case TC_KILL:
		case TC_CONNECT:
		case TC_DISCONNECT:
		case TC_MAP:
		case TC_UNMAP:
		case TC_STOPPING:
		case PTC_STOPPED:
		case PTC_STARTING:
		  if (done_status) {
		      send_component_status_ptc(requestor, tc.comp_ref, true, killed_status, tc.local_verdict, 
		    		  tc.return_type, tc.return_value);
		    } else {
		      send_component_status_ptc(requestor, tc.comp_ref, false,killed_status, tc.local_verdict, null, null);
		    }
		    break;
		  case PTC_STOPPING_KILLING:
		  case PTC_KILLING:
		  case TC_EXITING:
		  case TC_EXITED:
		    // the PTC requestor is not interested in the component status anymore
		    break;
		  default:
			  // TODO error
		  }
	}

	private static void send_component_status_ptc(ComponentStruct tc, int comp_ref, boolean is_done,
			boolean is_killed, VerdictTypeEnum local_verdict, String return_type, byte[] return_value) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_COMPONENT_STATUS);
		text_buf.push_int(comp_ref);
		text_buf.push_int(is_done ? 1 : 0);
		text_buf.push_int(is_killed ? 1 : 0);
		text_buf.push_int(local_verdict.getValue());
		text_buf.push_string(return_type);
		if (tc.return_value != null) {
			text_buf.push_raw(tc.return_value);
		}
		send_message(tc.comp_location, text_buf);		
	}

	private static void process_stop_req(ComponentStruct tc) {
		if (!request_allowed(tc, "STOP_REQ")) {
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		int component_reference = text_buf.pull_int().get_int();
		text_buf.cut_message();
		
		switch(component_reference) {
		case TitanComponent.NULL_COMPREF:
			send_error(tc.comp_location, "Stop operation was requested on the null component reference.");
			return;
		case TitanComponent.MTC_COMPREF:
			if (!tc.equals(mtc)) {
				if (!mtc.stop_requested) {
					send_stop(mtc);
					kill_all_components(true);
					mtc.stop_requested = true;
					// TODO timer
					System.out.println("Test Component "+tc.comp_ref+" had requested to stop MTC. Terminating current "
							+ "testcase execution.");
				}
			}
			else {
				send_error(tc.comp_location, "MTC has requested to stop itself.");
			}
			return;
		case TitanComponent.SYSTEM_COMPREF:
			send_error(tc.comp_location, "Stop operation was requested on the component reference of the system.");
			return;
		case TitanComponent.ANY_COMPREF:
			send_error(tc.comp_location, "Stop operation was requested on 'any component'.");
			return;
		case TitanComponent.ALL_COMPREF:
			if (tc.equals(mtc)) {
				if (stop_all_components()) {
					send_stop_ack(mtc);
				}
				else {
					mtc.tc_state = tcStateEnum.MTC_ALL_COMPONENT_STOP;
				}
			}
			else {
				send_error(tc.comp_location, "Operation 'all component.stop' can only be performed on the MTC.");
			}
			return;
		default:
			break;
		}
		ComponentStruct target = components.get(component_reference);
		if (target == null) {
			send_error(tc.comp_location, MessageFormat.format("The argument of stop operation is an invalid component "
					+ "reference {0}.", component_reference));
			return;
		}
		else if (target.equals(tc)) {
			send_error(tc.comp_location, "Stop operation was requested on the requestor component itself.");
			return;
		}
		boolean target_inactive = false;
		switch(target.tc_state) {
		case PTC_STOPPED:
			if (!target.is_alive) {
				throw new TtcnError("PTC "+component_reference+" cannot be in state STOPPED because it is not an "
						+ "alive type PTC."); 
			}
			// no break
		case TC_IDLE:
			target_inactive = true;
			// no break
		case TC_CREATE:
		case TC_START:
		case TC_STOP:
		case TC_KILL:
		case TC_CONNECT:
		case TC_DISCONNECT:
		case TC_MAP:
		case TC_UNMAP:
		case PTC_FUNCTION:
			if (target.is_alive) {
				if (target_inactive) {
					send_stop_ack(tc);
					break;
				}
				else {
					send_stop(target);
					target.tc_state = tcStateEnum.TC_STOPPING;
				}
			}
			else {
				send_kill(target);
				if (target_inactive) {
					target.tc_state = tcStateEnum.PTC_KILLING;
				}
				else {
					target.tc_state = tcStateEnum.PTC_STOPPING_KILLING;
				}
			}
			target.stop_requested = true;
			target.stop_requestors = init_requestors(tc);
			target.kill_requestors = init_requestors(null);
			// TODO timer
			tc.tc_state = tcStateEnum.TC_STOP;
			break;
		case PTC_KILLING:
			if (target.is_alive) {
				send_stop_ack(tc);
				break;
			}
			// no break
		case TC_STOPPING:
		case PTC_STOPPING_KILLING:
			add_requestor(target.stop_requestors, tc);
			tc.tc_state = tcStateEnum.TC_STOP;
			break;
		case TC_EXITING:
		case TC_EXITED:
			send_stop_ack(tc);
			break;
		case PTC_STARTING:
			send_error(tc.comp_location, MessageFormat.format("PTC with component reference {0} cannot be stopped "
					+ "because it is currently being started.", component_reference));
			break;
		case PTC_STALE:
			send_error(tc.comp_location, MessageFormat.format("The argument of stop operation ({0}) is a component "
					+ "reference that belongs to an earlier testcase.", component_reference));
			break;
		default:
			send_error(tc.comp_location, MessageFormat.format("The test component that the stop operation refers to "
					+ "({0}) is in invalid state.", component_reference));
		}
	}

	private static void send_stop_ack(ComponentStruct tc) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_STOP_ACK);
		send_message(tc.comp_location, text_buf);
	}

	private static boolean stop_all_components() {
		boolean ready_for_ack = true;
		for (int i = tc_first_comp_ref; i<=components.size(); i++) {
			ComponentStruct tc = components.get(i);
			switch(tc.tc_state) {
			case TC_INITIAL:
				if (!tc.is_alive) {
					ready_for_ack = false;
				}
				break;
			case TC_IDLE:
				if (!tc.is_alive) {
					send_kill(tc);
					tc.tc_state = tcStateEnum.PTC_KILLING;
					tc.stop_requested = true;
					tc.stop_requestors = init_requestors(null);
					tc.kill_requestors = init_requestors(null);
					ready_for_ack = false;
				}
				break;
			case TC_CREATE:
		    case TC_START:
		    case TC_STOP:
		    case TC_KILL:
		    case TC_CONNECT:
		    case TC_DISCONNECT:
		    case TC_MAP:
		    case TC_UNMAP:
		    case PTC_FUNCTION:
		    	if (tc.is_alive) {
		    		send_stop(tc);
		    		tc.tc_state = tcStateEnum.TC_STOPPING;
		    	}
		    	else {
		    		send_kill(tc);
		    		tc.tc_state = tcStateEnum.PTC_STOPPING_KILLING;
		    	}
		    	tc.stop_requested = true;
		    	tc.stop_requestors = init_requestors(null);
		    	tc.killed_requestors = init_requestors(null);
		    	// TODO timer
		    	ready_for_ack = false;
		    	break;
		    case PTC_STARTING:
		    	tc.cancel_done_sent_to = new RequestorStruct();
		    	tc.tc_state = tcStateEnum.PTC_STOPPED;
		    	break;
		    case TC_STOPPING:
		    case PTC_STOPPING_KILLING:
		    	tc.stop_requestors = new RequestorStruct();
		    	tc.killed_requestors = new RequestorStruct();
		    	ready_for_ack = false;
		    	break;
		    case PTC_KILLING:
		    	tc.stop_requestors = new RequestorStruct();
		    	tc.killed_requestors = new RequestorStruct();
		        if (!tc.is_alive) {
		        	ready_for_ack = false;
		        }
		        break;
		    case PTC_STOPPED:
		    case TC_EXITING:
		    case TC_EXITED:
		    case PTC_STALE:
		    	break;
		    default:
		    	throw new TtcnError("Test Component "+tc.comp_ref+" is in invalid state when stopping all components.");
		      
			}
			boolean mtc_requested_done = has_requestor(tc.done_requestors, mtc);
			tc.done_requestors = new RequestorStruct();
			if (mtc_requested_done) {
				add_requestor(tc.done_requestors, mtc);
			}
			boolean mtc_requested_killed = has_requestor(tc.killed_requestors, mtc);
			tc.killed_requestors = new RequestorStruct();
			if (mtc_requested_killed) {
				add_requestor(tc.killed_requestors, mtc);
			}
			tc.cancel_done_sent_for = new RequestorStruct();
		}
		return ready_for_ack;
	}

	private static boolean kill_all_components(boolean testcase_ends) {
		boolean ready_for_ack = true;
		for (int i = tc_first_comp_ref; i<=components.size(); i++) {
			ComponentStruct tc = components.get(i);
			boolean is_inactive = false;
			switch(tc.tc_state) {
			case TC_INITIAL:
				ready_for_ack = false;
				break;
			case PTC_STARTING: 
				tc.cancel_done_sent_to = new RequestorStruct();
				// no break
			case TC_IDLE:
		    case PTC_STOPPED:
		      is_inactive = true;
		      // no break
		    case TC_CREATE:
		    case TC_START: 
		    case TC_STOP:
		    case TC_KILL:
		    case TC_CONNECT:
		    case TC_DISCONNECT:
		    case TC_MAP:
		    case TC_UNMAP:
		    case PTC_FUNCTION:
		    	send_kill(tc);
		    	if (is_inactive) {
		    		tc.tc_state = tcStateEnum.PTC_KILLING;
		    		if (!tc.is_alive) {
		    			tc.stop_requested = true;
		    		}
		    	}
		    	else {
		    		tc.tc_state = tcStateEnum.PTC_STOPPING_KILLING;
		    		tc.stop_requested = true;
		    	}
		    	tc.stop_requestors = init_requestors(null);
		    	tc.kill_requestors = init_requestors(null);
		    	// TODO timer  
		    	ready_for_ack = false;
		    	break;
		    case TC_STOPPING:
		    	send_kill(tc);
		    	tc.tc_state = tcStateEnum.PTC_STOPPING_KILLING;
		    	// TODO timer
		    	// no break
		    case PTC_KILLING:
		    case PTC_STOPPING_KILLING:
		    	tc.stop_requestors = new RequestorStruct();
		    	tc.kill_requestors = new RequestorStruct();
		    	ready_for_ack = false;
		    	break;
		    case TC_EXITING:
		    	if (testcase_ends) {
		    		ready_for_ack = false;
		    	}
		    case TC_EXITED:
		    case PTC_STALE:
		    	break;
	    	default:
	    		// TODO error
			}
			if (testcase_ends) {
				tc.done_requestors = new RequestorStruct();
				tc.killed_requestors = new RequestorStruct();
			}
			else {
				boolean mtc_requested_done = has_requestor(tc.done_requestors, mtc);
				tc.done_requestors = new RequestorStruct();
				if (mtc_requested_done) {
					add_requestor(tc.done_requestors, mtc);
				}
				boolean mtc_requested_killed = has_requestor(tc.killed_requestors, mtc);
				tc.killed_requestors = new RequestorStruct();
				if (mtc_requested_killed) {
					add_requestor(tc.killed_requestors, mtc);
				}
			}
			tc.cancel_done_sent_for = new RequestorStruct();
		}
		return ready_for_ack;		
	}

	private static boolean has_requestor(RequestorStruct reqs, ComponentStruct tc) {
		switch(reqs.n_components) {
		case 0:
			return false;
		case 1:
			return reqs.comp.equals(tc);
		default:
			for (ComponentStruct comp : reqs.components) {
				if (comp.equals(tc)) {
					return true;
				}
			}
			return false;
		}
	}

	private static void send_stop(ComponentStruct tc) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_STOP);
		send_message(tc.comp_location, text_buf);
	}

	private static void process_disconnected(ComponentStruct tc) {
		if (!message_expected(tc, "DISCONNECTED")) {
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		String sourcePort = text_buf.pull_string();
		int remoteComponent = text_buf.pull_int().get_int();
		String remotePort = text_buf.pull_string();
		text_buf.cut_message();
		
		PortConnection conn = find_connection(tc.comp_ref, sourcePort, remoteComponent, remotePort);
		if (conn != null) {
			switch(conn.conn_state) {
			case CONN_LISTENING:
				if (conn.headComp != tc.comp_ref || conn.headPort.equals(sourcePort)) {
					send_error(tc.comp_location, MessageFormat.format("Unexpected message DISCONNECTED was received "
							+ "for port connection {0}:{1} - {2}:{3}.", tc.comp_ref, sourcePort, remoteComponent, remotePort));
					break;
				}
				// no break
			case CONN_CONNECTING:
				send_error_to_connect_requestors(conn, "test component "+tc.comp_ref+" reported end of the connection "
						+ "during connection setup.");
				remove_connection(conn);
				break;
			case CONN_CONNECTED:
				remove_connection(conn);
				break;
			case CONN_DISCONNECTING:
				send_disconnect_ack_to_requestors(conn);
				remove_connection(conn);
				break;
			default:
				// TODO error
			}
		}
	}

	private static void send_disconnect_ack_to_requestors(PortConnection conn) {
		for (int i=0; ; i++) {
			ComponentStruct comp = get_requestor(conn.requestors, i);
			if (comp == null) {
				break;
			}
			else if (comp.tc_state == tcStateEnum.TC_DISCONNECT) {
				send_disconnect_ack(comp);
				if (comp.equals(mtc)) {
					comp.tc_state = tcStateEnum.MTC_TESTCASE;
				}
				else {
					comp.tc_state = tcStateEnum.PTC_FUNCTION;
				}
			}
		}
		
		conn.requestors = new RequestorStruct();
	}

	private static void process_disconnect_req(ComponentStruct tc) {
		if (!request_allowed(tc, "DISCONNECT_REQ")) {
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		int sourceComponent = text_buf.pull_int().get_int();
		String sourcePort = text_buf.pull_string();
		int destinationComponent = text_buf.pull_int().get_int();
		String destinationPort = text_buf.pull_string();
		text_buf.cut_message();
		
		if (!valid_endpoint(sourceComponent, false, tc, "disconnect") || 
				!valid_endpoint(destinationComponent, false, tc, "disconnect")) {
			return;
		}
		PortConnection conn = find_connection(sourceComponent, sourcePort, destinationComponent, destinationPort);
		if (conn != null) {
			switch(conn.conn_state) {
			case CONN_LISTENING:
			case CONN_CONNECTING:
				send_error(tc.comp_location, MessageFormat.format("The port connection {0}:{1} - {2}:{3} cannot "
						+ "be destroyed because a connect operation is in progress on it.", sourceComponent, 
						sourcePort, destinationComponent, destinationPort));
				break;
			case CONN_CONNECTED:
				send_disconnect(components.get(conn.tailComp), conn.tailPort, conn.headComp, conn.headPort);
				conn.conn_state = connStateEnum.CONN_DISCONNECTING;
				// no break
			case CONN_DISCONNECTING:
				add_requestor(conn.requestors, tc);
				tc.tc_state = tcStateEnum.TC_DISCONNECT;
				break;
			default:
				send_error(tc.comp_location, MessageFormat.format("The port connection {0}:{1} - {2}:{3} cannot "
						+ "be destroyed due to an internal error in the MC.", sourceComponent, 
						sourcePort, destinationComponent, destinationPort));
				// TODO error
			}
		}
		else {
			send_disconnect_ack(tc);
		}
	}

	private static void send_disconnect_ack(ComponentStruct tc) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_DISCONNECT_ACK);
		send_message(tc.comp_location, text_buf);
	}

	private static void send_disconnect(ComponentStruct tc, String tailPort, int headComp,
			String headPort) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_DISCONNECT);
		text_buf.push_string(tailPort);
		text_buf.push_int(headComp);
		text_buf.push_string(headPort);
		
		send_message(tc.comp_location, text_buf);
	}

	private static void process_start_req(ComponentStruct tc) {
		if (!request_allowed(tc, "START_REQ")) {
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		int component_reference = text_buf.pull_int().get_int();
		String module_name = text_buf.pull_string();
		String function_name = text_buf.pull_string();
		
				
		switch(component_reference) {
		case TitanComponent.NULL_COMPREF:
			send_error(tc.comp_location, "Start operation was requested on the null component reference.");
			return;
		case TitanComponent.MTC_COMPREF:
			send_error(tc.comp_location, "Start operation was requested on the component reference of the MTC.");
			return;
		case TitanComponent.SYSTEM_COMPREF:
			send_error(tc.comp_location, "Start operation was requested on the component reference of the system.");
			return;
		case TitanComponent.ANY_COMPREF:
			send_error(tc.comp_location, "Start operation was requested on 'any component'.");
			return;
		case TitanComponent.ALL_COMPREF:
			send_error(tc.comp_location, "Start operation was requested on 'all component'.");
			return;
		}

		ComponentStruct target = components.get(component_reference);
		if (target == null) {
			send_error(tc.comp_location, MessageFormat.format("Start operation was requested on invalid component reference {0}.", component_reference));
			return;
		}
		switch(target.tc_state) {
		case TC_IDLE:
		case PTC_STOPPED:
			break;
		case TC_CREATE:
		case TC_START:
		case TC_STOP:
		case TC_KILL:
		case TC_CONNECT:
		case TC_DISCONNECT:
		case TC_MAP:
		case TC_UNMAP:
		case PTC_FUNCTION:
		case PTC_STARTING:
			send_error(tc.comp_location, MessageFormat.format("PTC with component reference {0} cannot be started "
					+ "because it is already executing function {1}.{2}.", component_reference, tc.tc_fn_name.module_name, tc.tc_fn_name.definition_name));
			return;
		case TC_STOPPING:
			send_error(tc.comp_location, MessageFormat.format("PTC with component reference {0} cannot be started "
					+ "because its function {1}.{2} is currently being stopped on it.", component_reference, tc.tc_fn_name.module_name, tc.tc_fn_name.definition_name));
			return;
		case PTC_KILLING:
		case PTC_STOPPING_KILLING:
			send_error(tc.comp_location, MessageFormat.format("PTC with component reference {0} cannot be started "
					+ "because it is currently being killed.", component_reference));
			return;
		case TC_EXITING:
		case TC_EXITED:
			send_error(tc.comp_location, MessageFormat.format("PTC with component reference {0} cannot be started "
					+ "because it is not alive anymore.", component_reference));
			return;
		case PTC_STALE:
			send_error(tc.comp_location, MessageFormat.format("The argument of starte operation ({0}) is a component "
					+ "reference that belongs to an earlier state.", component_reference));
			return;
		default:
			send_error(tc.comp_location, MessageFormat.format("Start operation was requested on component reference {0} "
					+ "which is in invalid state. ", component_reference));
			return;
		}
		target.tc_fn_name = new QualifiedName(module_name, function_name);
		boolean send_cancel_done = false; 
		boolean cancel_any_component_done = false;
		
		/*
		TitanInteger argTitan = new TitanInteger();
		int arg = -1; 
		if (text_buf.safe_pull_int(argTitan)) {
			//text_buf.pull_int().get_int();
			arg = argTitan.get_int();
		}
		
		// int arg = -1;
		byte return_value[] = new byte[text_buf.get_len() - text_buf.get_pos()];
		text_buf.pull_raw(text_buf.get_len() - text_buf.get_pos(), return_value);
		text_buf.cut_message();
		*/
		byte[] arg = new byte[text_buf.get_len() - text_buf.get_pos()];
		text_buf.pull_raw(text_buf.get_len() - text_buf.get_pos(), arg);
		
		if (target.tc_state == tcStateEnum.PTC_STOPPED) {
			target.tc_state = tcStateEnum.PTC_STARTING;
			target.return_type = null;
			// target.return_value = return_value;
			target.return_value = null;
			target.cancel_done_sent_to = init_requestors(null);
			for (int i=0; ; i++) {
				ComponentStruct comp = get_requestor(target.done_requestors, i);
				if (comp == null) {
					break;
				}
				else if (comp.equals(tc)) {
					continue;
				}
				switch (comp.tc_state) {
			    case TC_CREATE:
			    case TC_START:
			    case TC_STOP:
			    case TC_KILL:
			    case TC_CONNECT:
			    case TC_DISCONNECT:
			    case TC_MAP:
			    case TC_UNMAP:
			    case TC_STOPPING:
			    case MTC_TESTCASE:
			    case PTC_FUNCTION:
			    case PTC_STARTING:
			    case PTC_STOPPED:
			    	send_cancel_done = true;
			    	add_requestor(target.cancel_done_sent_to, comp);
			    	break;
			    case TC_EXITING:
			    case TC_EXITED:
			    case PTC_KILLING:
			    case PTC_STOPPING_KILLING:
			    	break;
			    default:
			    	// TODO error 
				}
			}
			
			if (any_component_done_sent.get() && !is_any_component_done()) {
				send_cancel_done = true;
				cancel_any_component_done = true;
				any_component_done_sent.set(false);
				add_requestor(target.cancel_done_sent_to, mtc);
			}
			target.done_requestors = new RequestorStruct();
		}
		if (send_cancel_done) {
			for (int i=0; ; i++) {
				ComponentStruct comp = get_requestor(target.cancel_done_sent_to, i);
				if (comp == null) {
					break;
				}
				else if (comp.equals(mtc)) {
					send_cancel_done_mtc(component_reference, cancel_any_component_done);
				}
				else {
					send_cancel_done_ptc(comp, component_reference);
				}
				add_requestor(comp.cancel_done_sent_for, target);
			}
			target.start_requestor = tc;
			tc.tc_state = tcStateEnum.TC_START;
			tc.arg = arg;
		}
		else {
			send_start(target, target.tc_fn_name, arg);
			send_start_ack(tc);
			target.tc_state = tcStateEnum.PTC_FUNCTION;
		}
	}

	private static void send_start_ack(ComponentStruct tc) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_START_ACK);
		
		send_message(tc.comp_location, text_buf);
	}

	//private static void send_start(ComponentStruct target, QualifiedName tc_fn_name, int arg) {
	private static void send_start(ComponentStruct target, QualifiedName tc_fn_name, byte[] arg) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_START);
		text_buf.push_string(tc_fn_name.module_name);
		text_buf.push_string(tc_fn_name.definition_name);
		//text_buf.push_int(arg);
		text_buf.push_raw(arg);

		send_message(target.comp_location, text_buf);
	}

	private static void send_cancel_done_ptc(ComponentStruct comp, int component_reference) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_CANCEL_DONE);
		text_buf.push_int(component_reference);

		send_message(mtc.comp_location, text_buf);
	}

	private static void send_cancel_done_mtc(int component_reference, boolean cancel_any_component_done) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_CANCEL_DONE);
		text_buf.push_int(component_reference);
		text_buf.push_int(cancel_any_component_done ? 1 : 0);

		send_message(mtc.comp_location, text_buf);
	}

	private static boolean is_any_component_done() {
		for (int i = tc_first_comp_ref; i<=components.size(); i++) {
			ComponentStruct comp = components.get(i);
			if (component_is_done(comp)) {
				return true;
			}
		}

		return false;
	}

	private static boolean component_is_done(ComponentStruct tc) {
		switch(tc.tc_state) {
		case TC_EXITING:
		case TC_EXITED:
		case PTC_STOPPED:
			return true;
		case TC_INITIAL:
		case TC_IDLE:
		case TC_CREATE:
		case TC_START:
		case TC_STOP:
		case TC_KILL:
		case TC_CONNECT:
		case TC_DISCONNECT:
		case TC_MAP:
		case TC_UNMAP:
		case TC_STOPPING:
		case PTC_FUNCTION:
		case PTC_STARTING:
		case PTC_KILLING:
		case PTC_STOPPING_KILLING:
			return false;
		default:
			// TODO error
			return false;
		}
	}

	private static void process_connected(ComponentStruct tc) {
		if (!message_expected(tc, "CONNECTED")) {
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		String local_port = text_buf.pull_string();
		int remote_component = text_buf.pull_int().get_int();
		String remote_port = text_buf.pull_string();
		text_buf.cut_message();
		
		PortConnection conn = find_connection(tc.comp_ref, local_port, remote_component, remote_port);
		if (conn != null) {
			if (conn.conn_state == connStateEnum.CONN_CONNECTING && conn.headComp == tc.comp_ref &&
					conn.headPort.equals(local_port)) {
				send_connect_ack_to_requestors(conn);
				conn.conn_state = connStateEnum.CONN_CONNECTED;
			}
			else {
				send_error(tc.comp_location, MessageFormat.format("Unexpected CONNECTED message was received "
						+ "for port connection {0}:{1} - {2}:{3}.", tc.comp_ref, local_port, remote_component, remote_port));
			}
		}
	}

	private static void send_connect_ack_to_requestors(PortConnection conn) {
		for (int i=0; ; i++) {
			ComponentStruct comp = get_requestor(conn.requestors, i);
			if (comp == null) {
				break;
			}
			else if (comp.tc_state == tcStateEnum.TC_CONNECT) {
				send_connect_ack(comp);
				if (comp.equals(mtc)) {
					comp.tc_state = tcStateEnum.MTC_TESTCASE;
				}
				else {
					comp.tc_state = tcStateEnum.PTC_FUNCTION;
				}
			}
		}
		
		conn.requestors.components = null;
		conn.requestors.n_components = 0;
	}

	private static void process_connect_listen_ack(ComponentStruct tc, int msg_end) {
		if (!message_expected(tc, "CONNECT_LISTEN_ACK")) {
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		String local_port = text_buf.pull_string();
		int remote_component = text_buf.pull_int().get_int();
		String remote_port = text_buf.pull_string();
		int transport_type = text_buf.pull_int().get_int();
		
		final byte temp[] = new byte[2];
		final byte local_port_number[] = new byte[2];
		text_buf.pull_raw(2, temp);
		text_buf.pull_raw(2, local_port_number);
		int local_addr_begin = text_buf.get_pos();
		int local_addr_len = msg_end - local_addr_begin;
		
		byte addr[];
		if (local_addr_len == 12) {
			addr = new byte[4];
			text_buf.pull_raw(4, addr);
			final byte zero[] = new byte[8];
			text_buf.pull_raw(8, zero);			
		}
		else {
			addr = new byte[16];
			text_buf.pull_raw(16, addr);
		}
		InetAddress address = null;
		try {
			address = InetAddress.getByAddress(addr);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		text_buf.cut_message();
		
		PortConnection conn = find_connection(tc.comp_ref, local_port, remote_component, remote_port);
		if (conn != null) {
			if (conn.conn_state != connStateEnum.CONN_LISTENING || conn.headComp != tc.comp_ref 
					|| conn.headPort.compareTo(local_port) != 0) {
				send_error(tc.comp_location, MessageFormat.format("Unexpected message CONNECT_LISTEN_ACK was received "
						+ "for port connection {0}:{1} - {2}:{3}.", tc.comp_ref, local_port, remote_component, remote_port));
				return;
			}
			else if (conn.transport_type.ordinal() != transport_type) {
				send_error(tc.comp_location, MessageFormat.format("Message CONNECT_LISTEN_ACK for port connection "
						+ "{0}:{1} - {2}:{3} contains wrong transport type: {4} was expected instead of {5}.", 
						tc.comp_ref, local_port, remote_component, remote_port, conn.transport_type.toString(), transport_type_enum.values()[transport_type].toString()));
				return;
			}
			ComponentStruct dst_comp = components.get(remote_component);
			switch(dst_comp.tc_state) {
			case TC_IDLE:
		    case TC_CREATE:
		    case TC_START:
		    case TC_STOP:
		    case TC_KILL:
		    case TC_CONNECT:
		    case TC_DISCONNECT:
		    case TC_MAP:
		    case TC_UNMAP:
		    case TC_STOPPING:
		    case MTC_TESTCASE:
		    case PTC_FUNCTION:
		    case PTC_STARTING:
		    case PTC_STOPPED:
		    	if (tc.comp_ref != TitanComponent.MTC_COMPREF && tc.comp_ref != remote_component) {
		    		send_connect(dst_comp, remote_port, tc.comp_ref, tc.comp_name, local_port, 
		    				transport_type_enum.values()[transport_type], address, local_port_number);
		    	}
		    	else {
		    		send_connect(dst_comp, remote_port, tc.comp_ref, "", local_port, 
		    				transport_type_enum.values()[transport_type], address, local_port_number);
		    	}
		    	conn.conn_state = connStateEnum.CONN_CONNECTING;
		    	break;
		    default:
		    	send_disconnect_to_server(conn);
		    	send_error_to_connect_requestors(conn, "test component "+dst_comp+" has terminated during connection setup.");
		    	remove_connection(conn);
		    	break;
			}
		}
		else {
			switch(transport_type_enum.values()[transport_type]) {
			case TRANSPORT_LOCAL:
				send_error(tc.comp_location, MessageFormat.format("Message CONNECT_LISTEN_ACK for port connection "
						+ "{0}:{1} - {2}:{3} cannot refer to transport type {4}.", tc.comp_ref, local_port, 
						remote_component, remote_port, transport_type_enum.values()[transport_type].toString()));
				
				break;
			case TRANSPORT_INET_STREAM:
			case TRANSPORT_UNIX_STREAM:
				break;
			default:
				send_error(tc.comp_location, MessageFormat.format("Message CONNECT_LISTEN_ACK for port connection "
						+ "{0}:{1} - {2}:{3} refers to invalid transport type {4}.", tc.comp_ref, local_port, 
						remote_component, remote_port, transport_type_enum.values()[transport_type].toString()));
				break;
			}
		}		
	}

	private static void send_error_to_connect_requestors(PortConnection conn, String msg) {
		String reason = "Establishment of port connection "+conn.comp_ref+":"+conn.port_name+" - "+conn.tailComp+":"
				+conn.tailPort+" failed because "+msg;
		for (int i=0; ;  i++) {
			ComponentStruct comp = get_requestor(conn.requestors, i);
			if (comp == null) {
				break;
			}
			else if (comp.tc_state == tcStateEnum.TC_CONNECT) {
				send_error(comp.comp_location, reason);
				if (comp.equals(mtc)) {
					comp.tc_state = tcStateEnum.MTC_TESTCASE;
				}
				else {
					comp.tc_state = tcStateEnum.PTC_FUNCTION;
				}
			}
		}				
	}

	private static ComponentStruct get_requestor(RequestorStruct reqs, int index) {
		if (index >= 0 && index < reqs.n_components) {
			if (reqs.n_components == 1) {
				return reqs.comp;
			}
			else {
				return reqs.components.get(index);
			}
		}
		return null;
	}

	private static void process_connect_error(ComponentStruct tc) {
		if (!message_expected(tc, "CONNECT_ERROR")) {
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		String localPort = text_buf.pull_string();
		int remote_comp = text_buf.pull_int().get_int();
		String remote_port = text_buf.pull_string();
		String message = text_buf.pull_string();
		
		text_buf.cut_message();
		
		PortConnection conn = find_connection(tc.comp_ref, localPort, remote_comp, remote_port);
		if (conn != null) {
			switch(conn.conn_state) {
			case CONN_CONNECTING:
				if (conn.transport_type != transport_type_enum.TRANSPORT_LOCAL && conn.tailComp == tc.comp_ref 
				&& conn.tailPort.equals(localPort)) {
					send_disconnect_to_server(conn);
				}
				break;
			case CONN_LISTENING:
				if (conn.headComp == tc.comp_ref && conn.headPort.equals(localPort)) {
					break;
				}
			default:
				send_error(tc.comp_location, MessageFormat.format("Unexpected message CONNECT_ERROR was received for "
						+ "port connection {0}:{1} - {2}:{3}.", tc.comp_ref, localPort, remote_comp, remote_port));
				return;
			}
			send_error_to_connect_requestors(conn, "test component "+tc.comp_ref+" reported error: "+message);
			remove_connection(conn);
		}		
	}

	private static void remove_connection(PortConnection c) {
		components.get(c.headComp).conn_head_list.remove(c);
		components.get(c.tailComp).conn_tail_list.remove(c);
	}

	private static void send_disconnect_to_server(PortConnection conn) {
		ComponentStruct comp = components.get(conn.headComp);
		switch (comp.tc_state) {
		case TC_IDLE:
		case TC_CREATE:
		case TC_START:
		case TC_STOP:
		case TC_KILL:
		case TC_CONNECT:
		case TC_DISCONNECT:
		case TC_MAP:
		case TC_UNMAP:
		case TC_STOPPING:
		case MTC_TESTCASE:
		case PTC_FUNCTION:
		case PTC_STARTING:
		case PTC_STOPPED:
			send_disconnect(comp, conn.headPort, conn.tailComp, conn.tailPort);
		default:
		    break;
		}
		
	}


	private static void process_connect_req(ComponentStruct tc) {
		if (!request_allowed(tc, "CONNECT_REQ")) return;
		
		Text_Buf text_buf = incoming_buf.get();
		int sourceComponent = text_buf.pull_int().get_int();
		String sourcePort = text_buf.pull_string();
		int destinationComponent = text_buf.pull_int().get_int();
		String destinationPort = text_buf.pull_string();

		text_buf.cut_message();

		if (!valid_endpoint(sourceComponent, true, tc, "connect") ||
				!valid_endpoint(destinationComponent, true, tc, "connect")) {
			return;
		}
		
		PortConnection conn = find_connection(sourceComponent, sourcePort, destinationComponent, destinationPort);
		if (conn == null) {
			conn = new PortConnection();
			conn.transport_type = choose_port_connection_transport(sourceComponent, destinationComponent);
			conn.headComp = sourceComponent;
			conn.headPort = sourcePort;
			conn.tailComp = destinationComponent;
			conn.tailPort = destinationPort;
			conn.requestors = init_requestors(tc);
			add_connection(conn);
			
			switch(conn.transport_type) {
			case TRANSPORT_LOCAL:
				send_connect(components.get(conn.headComp), conn.headPort, conn.tailComp, "", conn.tailPort, conn.transport_type, null, null);
				conn.conn_state = connStateEnum.CONN_CONNECTING;
				break;
			case TRANSPORT_UNIX_STREAM:
			case TRANSPORT_INET_STREAM:
				if (conn.tailComp != TitanComponent.MTC_COMPREF && conn.tailComp != conn.headComp) {
					send_connect_listen(components.get(conn.headComp), conn.headPort, conn.tailComp, 
							components.get(conn.tailComp).comp_name, conn.tailPort, conn.transport_type);
				}
				else {
					send_connect_listen(components.get(conn.headComp), conn.headPort, conn.tailComp, "",
					          conn.tailPort, conn.transport_type);
				}
				conn.conn_state = connStateEnum.CONN_LISTENING;
				break;
			default:
				send_error(tc.comp_location, MessageFormat.format("The port connection {0}:{1} - {2}:{3} cannot "
						+ "be established because no suitable transport mechanism is available on the corrensponding host(s).", 
						sourceComponent, sourcePort, destinationComponent, destinationPort));
				return;
			}
			tc.tc_state = tcStateEnum.TC_CONNECT;
		}
		else {
			switch(conn.conn_state) {
			case CONN_LISTENING:
			case CONN_CONNECTING:
				add_requestor(conn.requestors, tc);
				tc.tc_state = tcStateEnum.TC_CONNECT;
				break;
			case CONN_CONNECTED:
				send_connect_ack(tc);
				break;
			case CONN_DISCONNECTING:
				send_error(tc.comp_location, MessageFormat.format("The port connection {0}:{1} - {2}:{3} cannot "
						+ "be established because a disconnect operation is in progress on it.", 
						sourceComponent, sourcePort, destinationComponent, destinationPort));
				break;
			default:
				send_error(tc.comp_location, MessageFormat.format("The port connection {0}:{1} - {2}:{3} cannot "
						+ "be established due to an internal error in the MC.", 
						sourceComponent, sourcePort, destinationComponent, destinationPort));
				// TODO error
			}
		}
	}
	
	private static void add_requestor(RequestorStruct reqs, ComponentStruct tc) {
		switch(reqs.n_components) {
		case 0:
			reqs.n_components = 1;
			reqs.comp = tc;
			break;
		case 1:
			if (!reqs.comp.equals(tc)) {
				reqs.n_components = 2;
				reqs.components = new ArrayList<ComponentStruct>();
				reqs.components.add(reqs.comp);
				reqs.components.add(tc);
			}
			break;
		default:
			for (ComponentStruct comp : reqs.components) {
				if (comp.equals(tc)) {
					return;
				}
			}
			reqs.n_components++;
			reqs.components.add(tc);
		}
	}

	private static void send_connect_listen(ComponentStruct tc, String headPort, int tailComp,
			String comp_name, String tailPort, transport_type_enum transport_type) {
		
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_CONNECT_LISTEN);
		text_buf.push_string(headPort);
		text_buf.push_int(tailComp);
		text_buf.push_string(comp_name);
		text_buf.push_string(tailPort);
		text_buf.push_int(transport_type.ordinal());
		
		send_message(tc.comp_location, text_buf);
	}

	private static void send_connect(ComponentStruct tc, String headPort, int tailComp, String compName,
			String tailPort, transport_type_enum transport_type, InetAddress address, byte[] local_port_number) {

		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_CONNECT);
		text_buf.push_string(headPort);
		text_buf.push_int(tailComp);
		text_buf.push_string(compName);
		text_buf.push_string(tailPort);
		text_buf.push_int(transport_type.ordinal());
		if (address != null) {
			if(address instanceof Inet4Address) { 
				final byte temp[] = address.getAddress();
				text_buf.push_raw(2, new byte[]{2, 0});
				text_buf.push_raw(local_port_number.length, local_port_number);
				text_buf.push_raw(temp.length, temp);
				text_buf.push_raw(8, new byte[8]);
			} else if (address instanceof Inet6Address) {
				final Inet6Address localipv6_address = (Inet6Address)address;
				final byte temp[] = localipv6_address.getAddress();			
				text_buf.push_raw(2, new byte[]{2, 3});
				text_buf.push_raw(local_port_number.length, local_port_number);
				text_buf.push_raw(temp.length, temp);
				text_buf.push_int(localipv6_address.getScopeId());
			}
		}
						
		send_message(tc.comp_location, text_buf);
	}

	private static void send_connect_ack(ComponentStruct tc) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_CONNECT_ACK);
		send_message(tc.comp_location, text_buf);
	}

	private static transport_type_enum choose_port_connection_transport(int sourceComponent, int destinationComponent) {
		Host headLoc = components.get(sourceComponent).comp_location;
		if (sourceComponent == destinationComponent && headLoc.transport_supported[transport_type_enum.TRANSPORT_LOCAL.ordinal()]) {
			return transport_type_enum.TRANSPORT_LOCAL; 
		}
		
		Host tailLoc = components.get(destinationComponent).comp_location;
		if (headLoc.equals(tailLoc) && headLoc.transport_supported[transport_type_enum.TRANSPORT_UNIX_STREAM.ordinal()]) {
			return transport_type_enum.TRANSPORT_UNIX_STREAM;
		}
		if (headLoc.transport_supported[transport_type_enum.TRANSPORT_INET_STREAM.ordinal()] && 
				tailLoc.transport_supported[transport_type_enum.TRANSPORT_INET_STREAM.ordinal()]) {
			return transport_type_enum.TRANSPORT_INET_STREAM;
		}
		
		return transport_type_enum.TRANSPORT_NUM;
	}

	private static PortConnection find_connection(int sourceComponent, String sourcePort, int destinationComponent,
			String destinationPort) {
		
		if (sourceComponent > destinationComponent) {
		    int tmp_comp = sourceComponent;
		    sourceComponent = destinationComponent;
		    destinationComponent = tmp_comp;
		    String tmp_port = sourcePort;
		    sourcePort = destinationPort;
		    destinationPort = tmp_port;
		  } else if (sourceComponent == destinationComponent && sourcePort.compareTo(destinationPort) > 0) {
		    String tmp_port = sourcePort;
		    sourcePort = destinationPort;
		    destinationPort = tmp_port;
		  }
		
		ComponentStruct headComp = components.get(sourceComponent);
		
		List<PortConnection> headConn = headComp.conn_head_list;
		if (headConn == null) {
			return null;
		}

		ComponentStruct tailComp = components.get(destinationComponent);
		List<PortConnection> tailConn = tailComp.conn_tail_list;
		if (tailConn == null) {
			return null;
		}	
		
		if(headComp.conn_head_list.size() <= tailComp.conn_tail_list.size()) {
			for (PortConnection pc : headConn) {
				if (pc.tailComp == destinationComponent && pc.headPort.equals(sourcePort) 
						&& pc.tailPort.equals(destinationPort)) {
					return pc;
				}
			}
			return null;
		} 
		else {
			for (PortConnection pc : tailConn) {
				if (pc.headComp == sourceComponent && pc.headPort.equals(sourcePort)
						&& pc.tailPort.equals(destinationPort)) {
					return pc;
				}
			}
			return null;
		}
	}
	
	private static void add_connection(PortConnection pc) {
		if (pc.headComp > pc.tailComp) {
			int tmp_comp = pc.headComp;
			pc.headComp = pc.tailComp;
			pc.tailComp = tmp_comp;
		    String tmp_port = pc.headPort;
		    pc.headPort = pc.tailPort;
		    pc.tailPort = tmp_port;
		}
		else if (pc.headComp == pc.tailComp && pc.headPort.compareTo(pc.tailPort) > 0) {
			String tmp_port = pc.headPort;
			pc.headPort = pc.tailPort;
			pc.tailPort = tmp_port;
		}
		
		ComponentStruct headComp = components.get(pc.headComp);
		if (headComp.conn_head_list == null) {
			headComp.conn_head_list = new ArrayList<PortConnection>();
		}
		headComp.conn_head_list.add(pc);

		ComponentStruct tailComp = components.get(pc.tailComp);
		if (tailComp.conn_tail_list == null) {
			tailComp.conn_tail_list = new ArrayList<PortConnection>();
		}
		tailComp.conn_tail_list.add(pc);		
	}

	private static boolean valid_endpoint(int component_reference, boolean new_connection, ComponentStruct requestor, 
			String operation) {
		switch(component_reference) {
		case TitanComponent.NULL_COMPREF:
			send_error(requestor.comp_location, MessageFormat.format("The {0} refers to the null component reference.", operation));
			return false;
		case TitanComponent.SYSTEM_COMPREF:
			send_error(requestor.comp_location, MessageFormat.format("The {0} refers to the system component reference.", operation));
			return false;
		case TitanComponent.ANY_COMPREF:
			send_error(requestor.comp_location, MessageFormat.format("The {0} refers to 'any component'.", operation));
			return false;
		case TitanComponent.ALL_COMPREF:
			send_error(requestor.comp_location, "The "+operation+" refers to 'all component'.");
			send_error(requestor.comp_location, MessageFormat.format("The {0} refers to 'all component'.", operation));
			return false;
		default:
			break;
		}
		
		ComponentStruct comp = components.get(component_reference);
		if (comp == null) {
			send_error(requestor.comp_location, MessageFormat.format("The {0} refers to invalid component "
					+ "reference {1}.", operation, component_reference));
			return false;
		}
		
		switch(comp.tc_state) {
		case TC_IDLE:
		case TC_CREATE:
		case TC_START:
		case TC_STOP:
		case TC_KILL:
		case TC_CONNECT:
		case TC_DISCONNECT:
		case TC_MAP:
		case TC_UNMAP:
		case TC_STOPPING:
		case MTC_TESTCASE:
		case PTC_FUNCTION:
		case PTC_STARTING:
		case PTC_STOPPED:
			return true;
		case PTC_KILLING:
		case PTC_STOPPING_KILLING:
			if (new_connection) {
				send_error(requestor.comp_location, MessageFormat.format("The {0} refers to test component with "
						+ "component reference {1}, which is currently being terminated.", operation, component_reference));
				return false;
		    } 
			else {
				return true;
			}
		case TC_EXITING:
		case TC_EXITED:
			if (new_connection) {
				send_error(requestor.comp_location, MessageFormat.format("The {0} refers to test component with "
						+ "component reference {1}, which has already terminated.", operation, component_reference));
		      return false;
		    } else {
		    	return true;
		    }
		case PTC_STALE:
			send_error(requestor.comp_location, MessageFormat.format("The {0} refers to component reference {1} "
					+ ", which belongs to an earlier test case.", operation, component_reference));
			return false;
		default:
			send_error(requestor.comp_location, MessageFormat.format("The {0} refers to component reference {1} "
					+ ", which is in invalid state.", operation, component_reference));
		    return false;
		}
		
	}

	private static void process_mtc_ready() {
		incoming_buf.get().cut_message();
		if(mc_state != mcStateEnum.MC_EXECUTING_CONTROL || mtc.tc_state != tcStateEnum.MTC_CONTROLPART) {
			send_error(mtc.comp_location, "Unexpected message MTC_READY was received.");
			return;
		}
		mc_state = mcStateEnum.MC_READY;
		mtc.tc_state = tcStateEnum.TC_IDLE;
		mtc.stop_requested = false;
		System.out.println("Test execution finished.");
		// TODO timer
	}

	  
	private static void shutdown_session() {
		switch(mc_state) {
		case MC_INACTIVE:
			break;
		case MC_SHUTDOWN:
			break;
		case MC_LISTENING:
		case MC_LISTENING_CONFIGURED:
		case MC_HC_CONNECTED:
		case MC_ACTIVE:
			System.out.println("Shutting down session.");
			perform_shutdown();
			break;
		default:
			// TODO error
		}
		
	}

	private static void process_ptc_created(Host hc) {
		switch(mc_state) {
		case MC_EXECUTING_TESTCASE:
		case MC_TERMINATING_TESTCASE:
			break;
		default:
			close_connection(hc);
			send_error(hc, "Message PTC_CREATED arrived in invalid state.");
			return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		int component_reference = text_buf.pull_int().get_int();
		text_buf.cut_message();
		
		switch(component_reference) {
		case TitanComponent.NULL_COMPREF:
			send_error(hc, "Message PTC_CREATED refers to the null component reference.");
			close_connection(hc);
		    return;
		case TitanComponent.MTC_COMPREF:
			send_error(hc, "Message PTC_CREATED refers to the component reference of the MTC.");
			close_connection(hc);
			return;
		case TitanComponent.SYSTEM_COMPREF:
			send_error(hc, "Message PTC_CREATED refers to the component reference of the system.");
			close_connection(hc);
		    return;
		case TitanComponent.ANY_COMPREF:
			send_error(hc, "Message PTC_CREATED refers to 'any component'.");
			close_connection(hc);
		    return;
		case TitanComponent.ALL_COMPREF: 
			send_error(hc, "Message PTC_CREATED refers to 'all component'.");
			close_connection(hc);
		    return;
		}

		ComponentStruct tc = components.get(component_reference);
		if (tc == null) {
			send_error(hc, MessageFormat.format("Message PTC_CREATED referes to invalid component reference {0}.", component_reference));
		}
		else if (tc.tc_state != tcStateEnum.TC_INITIAL) {
			send_error(hc, MessageFormat.format("Message PTC_CREATED refers to test component {0}, which is not "
					+ "being created.", component_reference));
		}
				
		tc.tc_state = tcStateEnum.TC_IDLE;		
		if(mc_state == mcStateEnum.MC_TERMINATING_TESTCASE || mtc.stop_requested || 
				mtc.tc_state == tcStateEnum.MTC_ALL_COMPONENT_KILL ||
				(mtc.tc_state == tcStateEnum.MTC_ALL_COMPONENT_STOP && !tc.is_alive)) {
			send_kill(tc);
			tc.tc_state = tcStateEnum.PTC_KILLING;
			if (!tc.is_alive) {
				tc.stop_requested = true;
			}
			tc.stop_requestors = init_requestors(null);
			tc.kill_requestors = init_requestors(null);
			// TODO start kill timer
		}
		else {
			if (tc.create_requestor.tc_state == tcStateEnum.TC_CREATE) {
				send_create_ack(tc.create_requestor, component_reference);
				if(tc.create_requestor.equals(mtc)) {
					tc.create_requestor.tc_state = tcStateEnum.MTC_TESTCASE;
				}
				else {
					tc.create_requestor.tc_state = tcStateEnum.PTC_FUNCTION;
				}
			}
		}
		handle_tc_data(tc);
	}
	
	private static void send_kill(ComponentStruct tc) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_KILL);
		send_message(tc.comp_location, text_buf);
	}

	private static void send_create_ack(ComponentStruct create_requestor, int component_reference) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_CREATE_ACK);
		text_buf.push_int(component_reference);
		send_message(create_requestor.comp_location, text_buf);
	}

	public static boolean request_allowed(ComponentStruct from, String message_name) {
	  if (!message_expected(from, message_name)) return false;

	  switch (from.tc_state) {
	  case MTC_TESTCASE:
		  if (from.equals(mtc)) {
			  return true;
		  }
	      else break;
	  case PTC_FUNCTION:
	      if (!from.equals(mtc)) {
	    	  return true;
	      }
	      else break;
	  case TC_STOPPING:
	  case PTC_STOPPING_KILLING:
	  case PTC_KILLING:
	    // silently ignore
	      return false;
	  default:
	      break;
	  } 
	  System.out.println(MessageFormat.format("The sender of message {0} is in unexpected state: {1}.", message_name, from.tc_state.toString()));
	  send_error(from.comp_location, MessageFormat.format("The sender of message {0} is in unexpected state.", message_name));
	  return false;
	}

	private static boolean message_expected(ComponentStruct from, String message_name) {
		switch (mc_state) {
		case MC_EXECUTING_TESTCASE:
			switch (mtc.tc_state) {
		    case MTC_ALL_COMPONENT_STOP:
		    case MTC_ALL_COMPONENT_KILL:
		      // silently ignore
		    	return false;
		    default:
		    	return true;
		    }
	    case MC_TERMINATING_TESTCASE:
	      // silently ignore
	    	return false;
	    default:
	    	send_error(from.comp_location, MessageFormat.format("Unexpected message {0} was received.", message_name));
	    	return false;
		}
	}

	private static void close_connection(Host hc) {
		try {
			hc.socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		hosts.remove(hc);
	}

	private static void process_testcase_finished() {
		if (mc_state != mcStateEnum.MC_EXECUTING_TESTCASE) {
			send_error(mtc.comp_location, "Unexpected message TESTCASE_FINISHED was received.");
			return;
		}
		boolean ready_to_finish = kill_all_components(true);

		Text_Buf local_incoming_buf = incoming_buf.get();	
		mc_state = mcStateEnum.MC_TERMINATING_TESTCASE;
		mtc.tc_state = tcStateEnum.MTC_TERMINATING_TESTCASE;
		int verdict = local_incoming_buf.pull_int().get_int();
		String reason = local_incoming_buf.pull_string();
		mtc.local_verdict = VerdictTypeEnum.values()[verdict];
		mtc.verdict_reason = reason;
		mtc.stop_requested = false;
		// TODO timer
		local_incoming_buf.cut_message();
		
		any_component_done_requested.set(false);
		any_component_done_sent.set(false);
		all_component_done_requested.set(false);
		any_component_killed_requested.set(false);
		all_component_killed_requested.set(false);
		
		if(ready_to_finish) {
			finish_testcase();
		}
	} 
	

	private static void finish_testcase() {
		if (stop_requested.get()) {
			send_ptc_verdict(false);
			send_stop(mtc);
			mtc.tc_state = tcStateEnum.MTC_CONTROLPART;
			mtc.stop_requested = true;
			// TODO timer
			mc_state = mcStateEnum.MC_EXECUTING_CONTROL;
		}
		else if (stop_after_tc.get()) {
			send_ptc_verdict(false);
			mtc.tc_state = tcStateEnum.MTC_PAUSED;
			mc_state = mcStateEnum.MC_PAUSED;
			System.out.println("Execution has been paused.");
		}
		else {
			send_ptc_verdict(true);
			mtc.tc_state = tcStateEnum.MTC_CONTROLPART;
			mc_state = mcStateEnum.MC_EXECUTING_CONTROL;
		}
		
		for (int i = tc_first_comp_ref; i<=components.size(); i++) {
			ComponentStruct comp = components.get(i);
			comp.tc_state = tcStateEnum.PTC_STALE;
		}
		mtc.local_verdict = VerdictTypeEnum.NONE;
	}

	private static void send_ptc_verdict(boolean continue_execution) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_PTC_VERDICT);
		int n_ptcs = 0;
		for (int i = tc_first_comp_ref; i<=components.size(); i++) {
			ComponentStruct comp = components.get(i);
			if (comp.tc_state != tcStateEnum.PTC_STALE) {
				n_ptcs++;
			}
		}
		text_buf.push_int(n_ptcs);
		for (int i = tc_first_comp_ref; i<=components.size(); i++) {
			ComponentStruct comp = components.get(i);
			if (comp.tc_state != tcStateEnum.PTC_STALE) {
				text_buf.push_int(comp.comp_ref);
				text_buf.push_string(comp.comp_name);
				text_buf.push_int(comp.local_verdict.getValue());
				text_buf.push_string(comp.verdict_reason);
			}
		}
		text_buf.push_int(continue_execution ? 1 : 0);
		send_message(mtc.comp_location, text_buf);
		incoming_buf.get().cut_message();
	}

	private static void process_testcase_started() { 
		if (mc_state != mcStateEnum.MC_EXECUTING_CONTROL) {
			send_error(mtc.comp_location, "Unexpected message TESTCASE_STARTED was received.");
		    return;
		}
		
		Text_Buf text_buf = incoming_buf.get();
		mtc.tc_fn_name = new QualifiedName(text_buf.pull_string(), text_buf.pull_string());
		mtc.comp_type = new QualifiedName(text_buf.pull_string(), text_buf.pull_string());
		system.comp_type = new QualifiedName(text_buf.pull_string(), text_buf.pull_string());
		mtc.tc_state = tcStateEnum.MTC_TESTCASE;
		mc_state = mcStateEnum.MC_EXECUTING_TESTCASE;
		tc_first_comp_ref = next_comp_ref;
				
		any_component_done_requested.set(false);
		any_component_done_sent.set(false);
		all_component_done_requested.set(false);
		any_component_killed_requested.set(false);
		all_component_killed_requested.set(false);
		text_buf.cut_message();
		incoming_buf.get().cut_message();;
	}

	private static void process_log(Host tc) {
		Text_Buf text_buf = incoming_buf.get();
		int seconds = text_buf.pull_int().get_int();
		int microseconds = text_buf.pull_int().get_int();
		int severity = text_buf.pull_int().get_int();
		
		int length = text_buf.pull_int().get_int();
		byte messageBytes[] = new byte[length];
		text_buf.pull_raw(length, messageBytes);
		System.out.println(new String(messageBytes));
		text_buf.cut_message();
	}

	private static void execute_testcase(Host hc, String moduleName, String testcaseName) {
		if (mc_state != mcStateEnum.MC_READY) {
		    // TODO error 
			return;
		}
		send_execute_testcase(hc, moduleName, testcaseName);
		mc_state = mcStateEnum.MC_EXECUTING_CONTROL;
		mtc.tc_state = tcStateEnum.MTC_CONTROLPART;
	}

	private static void send_execute_testcase(Host hc, String moduleName, String testcaseName) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_EXECUTE_TESTCASE);
		text_buf.push_string(moduleName);
		text_buf.push_string(testcaseName);
		send_message(hc, text_buf);
	}

	private static void execute_control(Host hc, String module_name) {
		if (mc_state != mcStateEnum.MC_READY) {
		    // TODO error
		    return;
		} 
		send_execute_control(hc, module_name);
	    mc_state = mcStateEnum.MC_EXECUTING_CONTROL;
	    mtc.tc_state = tcStateEnum.MTC_CONTROLPART;
	}
	
	private static void send_execute_control(Host hc, String module_name) {
		Text_Buf text_buf = new Text_Buf();
	    text_buf.push_int(MSG_EXECUTE_CONTROL);
	    text_buf.push_string(module_name);
	    send_message(hc, text_buf);
	}

	private static RequestorStruct init_requestors(ComponentStruct tc) {
		RequestorStruct requestors = new RequestorStruct();
		if (tc != null) {
			requestors.n_components = 1;
			requestors.comp = tc;
		}
		else {
			requestors.n_components = 0;
		}
		return requestors;
	}
	
	
	private static void exit_mtc() {
		if (mc_state != mcStateEnum.MC_READY && mc_state != mcStateEnum.MC_RECONFIGURING) {
			// TODO error
			return;
		}		
		System.out.println("Terminating MTC.");
		send_exit_mtc(); 
		
		process_final_log();
		mtc.tc_state = tcStateEnum.TC_EXITING;
		mc_state = mcStateEnum.MC_TERMINATING_MTC;
		// TODO timer
	}
	
	private static void process_final_log() {
		for (int i=0; i<2; i++) {
			receiveMessage(mtc.comp_location);
			Text_Buf local_incoming_buf = incoming_buf.get();
			final int msg_len = local_incoming_buf.pull_int().get_int();
			final int msg_type = local_incoming_buf.pull_int().get_int();
			process_log(mtc.comp_location);
		}
	}

	private static void send_exit_mtc() {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_EXIT_MTC);
		send_message(mtc.comp_location, text_buf);	
	}
	
	private static void perform_shutdown() {
		boolean shutdown_complete = true;
		switch(mc_state) {
		case MC_HC_CONNECTED:
		case MC_ACTIVE:
			for (Host host : hosts) {
				if (host.hc_state != hcStateEnum.HC_DOWN) {
					send_exit_hc(host);
					host.hc_state = hcStateEnum.HC_EXITING;
					shutdown_complete = false;
				}
			}
		case MC_LISTENING:
		case MC_LISTENING_CONFIGURED:
			// TODO shutdown server
			try {
				serverSocketChannel.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (shutdown_complete) {
				mc_state = mcStateEnum.MC_INACTIVE;
			}
			else {
				mc_state = mcStateEnum.MC_SHUTDOWN;
			}
			break;
		default: 
			// TODO error
		}
	}

	private static void send_exit_hc(Host hc) {
		Text_Buf text_buf = new Text_Buf();
		text_buf.push_int(MSG_EXIT_HC);
		send_message(hc, text_buf);
	}
	
}