package server.cli;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import server.info.ServerInfo;

/**
 * CLI is designed to talk to a MetaServer and ONLY to a MetaServer.
 * @author Plamen Jeliazkov
 *
 */
public class CommandLineInterface {
	private final Log LOG = LogFactory.getLog(CommandLineInterface.class);
	private final int META_PORT = 7400;
	
	private final Date START_TIME = new Date();
	private String IP_ADDRESS = null;
	private Socket META_SOCKET = null; //socket to connect to metaserver
	private PrintWriter STREAM_OUT = null;
	private ServerInfo INFO = null;
	private boolean SHUTDOWN = false;
	
	public CommandLineInterface() throws ConnectException {
		//get IP address
		Console c = System.console();
		if (c == null) {
			System.err.println("No console.");
			System.exit(1);
		}
		
		try {
			IP_ADDRESS = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			LOG.error("FATAL: Could not detect IP address:",e);
			System.exit(1);
		}
		
		//connect server socket
		try {
			META_SOCKET = new Socket(IP_ADDRESS,META_PORT);
		} catch (IOException e) {
			LOG.error("FATAL: Could not connect socket.",e);
			System.exit(1);
		}
		
		INFO = new ServerInfo(getAddress(), getPort(), 0L, ServerInfo.TYPES.CLI);
		
		//create a reader for the console, a reader for the server response, and an OOS for ServerInfo
		BufferedReader br_in = null;
		ObjectOutputStream first = null;
		
	    try {
	    	first = new ObjectOutputStream(META_SOCKET.getOutputStream());
		    br_in = new BufferedReader(new InputStreamReader(META_SOCKET.getInputStream()));
			STREAM_OUT = new PrintWriter(META_SOCKET.getOutputStream(),true);
		} catch (IOException e) {
			LOG.error("FATAL: Could not get output / input stream from Socket.",e);
			System.exit(1);
		}

	    //send the ServerInfo (let them know a CLI is connecting)
	    try {
			first.writeObject(getInfo());
		} catch (IOException e) {
			LOG.error("FATAL: Could not write ServerInfo from Socket.",e);
			System.exit(1);
		}

	    System.out.println("The console has been connected to the local MetaServer.\nWelcome.");

	    while(true) {
			try {
				Thread.sleep(1000);

				while(br_in.ready()) {
					String output = br_in.readLine();
					System.out.println(output);
				}

				if(SHUTDOWN == false) {
					String input = c.readLine("shell> ");
					if(input.equals("shutdown") || input.equals("logout")) {
						SHUTDOWN = true;
						System.out.println("Logging off... Farewell!\n(PRESS CTRL+C TO EXIT)");
					}
					STREAM_OUT.println(input);
				}
			} catch (IOException e) {
				LOG.error("FATAL: Could not read from system input.",e);
				System.exit(1);
			} catch (InterruptedException e) {
				LOG.error("Sleep interrupted.");
			}
		}
	}
	
	/**
	 * Always attempts to connect to a local MetaServer on port 7400.
	 */
	public static void main(String[] args) {
		try {
			new CommandLineInterface();
		} catch(ConnectException ex) {
			System.out.println("FATAL: Could not connect socket. No MetaServer to connect to.");
		}
	}

	public Date getStartTime() {
		return START_TIME;
	}
	
	public String getAddress() {
		return IP_ADDRESS;
	}

	public int getPort() {
		return META_PORT;
	}
	
	public ServerInfo getInfo() {
		return INFO;
	}
}
