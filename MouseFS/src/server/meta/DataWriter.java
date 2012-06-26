package server.meta;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import server.info.DataServerInfo;
import server.info.ServerInfo;

public class DataWriter implements Runnable {
	private Socket SOCKET = null;
	private String FILE = null;
	private String DATA = null;
	public static enum ACTIONS {CREATE, WRITE, HEAD, TAIL, APPEND, INFO, REMOVE, SHUTDOWN};
	private ACTIONS ACTION;
	private PrintWriter CLI_OUT = null;
	private MetaServer METASERVER = null;

	private final Log LOG = LogFactory.getLog(DataWriter.class);
	private boolean WAIT_FOR_READ = false;
	
	public DataWriter(Socket dataServer, String file, String data, ACTIONS action, PrintWriter console_out, MetaServer metaserver) {
		this.SOCKET = dataServer;
		this.FILE = file;
		this.DATA = data;
		this.ACTION = action;
		this.CLI_OUT = console_out;
		this.METASERVER = metaserver;
	}
	
	public void handleAction() {
		Thread t = new Thread(this);
		t.start();
		LOG.info("SUCCESS: On "+(new Date().toString())+", a DataWriter connection to "+ACTION.toString()+" to/from a DataServer has been made.");
	}
	
	/* THIS CLASS IS DESIGNED TO RUN ONE AND ONLY ONCE AND THEN EXIT THE THREAD!
	 * IT WILL PRINT OUT A COMMAND TO PERFORM -- AND THEN IT WILL READ AN INPUT BACK AND PRINT IT TO CONSOLE!
	 *  */
	@Override
	public void run() {
		PrintWriter out = null;
		BufferedReader br = null;
		ObjectInputStream first = null;
		try {
			first = new ObjectInputStream(SOCKET.getInputStream());
			br = new BufferedReader(new InputStreamReader(SOCKET.getInputStream()));
			out = new PrintWriter(new OutputStreamWriter(SOCKET.getOutputStream()), true);
		} catch (IOException e) {
			LOG.error("FATAL: Could not get output stream from Socket.");
		}
		
		ServerInfo serverInfo = null;
		
		try {
			serverInfo = (ServerInfo) first.readObject();
		} catch (ClassNotFoundException | IOException e) {
			LOG.error("FATAL: Could not read the ServerInfo object.",e);
		}
		
		if(serverInfo.getType() != ServerInfo.TYPES.DATA) {
			LOG.error("FATAL: Did not connect to a DataServer!");
			try {
				br.close();
				first.close();
			} catch (IOException e) {
				LOG.error("FATAL: Could not close the OutputStreams.",e);
			}

			stop();
		}
		
		if(ACTION == ACTIONS.WRITE) {
			out.println("write "+FILE+" "+DATA);
		} else if(ACTION == ACTIONS.APPEND) {
			out.println("append "+FILE+" "+DATA);
		} else if(ACTION == ACTIONS.HEAD) {
			out.println("head "+FILE);
			WAIT_FOR_READ = true;
		} else if(ACTION == ACTIONS.TAIL) {
			out.println("tail "+FILE);
			WAIT_FOR_READ = true;
		} else if(ACTION == ACTIONS.CREATE) {
			out.println("create "+FILE);
		} else if(ACTION == ACTIONS.REMOVE) {
			out.println("remove"+FILE);
		} else if(ACTION == ACTIONS.INFO) {
			out.println("sysinfo");
			try {
				DataServerInfo dataServerUpdate = (DataServerInfo) first.readObject();
				METASERVER.updateDataServer(dataServerUpdate);
			} catch (ClassNotFoundException e) {
				LOG.error("FATAL: Could not cast ServerInfo to DataServerInfo.");
			} catch (IOException e) {
				LOG.error("FATAL: Could not read in the ServerInfo object.");
			}
		} else if(ACTION == ACTIONS.SHUTDOWN) {
			out.println("shutdown");
			WAIT_FOR_READ = true;
		} else {
			out.println("DataWriter could not determine what action to take.");
		}	
		
		try {
			String outputLine = null;
			
			/**
			 * THIS IS HORRIBLE BECAUSE WE READ THE DATA TWICE AND PASS IT THROUGH THE STREAMS TWICE!
			 * WE READ FROM DATASERVER -- PASS TO METASERVER -- WHICH PASSES TO CLI -- THIS IS BAD PRACTICE!!
			 */
			while(br.ready() || WAIT_FOR_READ) {
				outputLine = br.readLine();
					if(outputLine != null) {
					//print the first line returned back and then exit!
					CLI_OUT.println(outputLine);
					WAIT_FOR_READ = false;
				}
			}	
		} catch (IOException e) {
			LOG.error("Error when waiting trying to get console output to return from DataServer.",e);
		}
		stop();
	}
	
	public void stop() {
        LOG.info("SUCCESS: On "+(new Date().toString())+", a DataWriter finished writing to DataServer.");
    }
}
