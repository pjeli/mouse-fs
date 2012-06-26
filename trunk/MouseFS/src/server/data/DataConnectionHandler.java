package server.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DataConnectionHandler implements Runnable {
	private DataServer DATASERVER = null;
	private Socket SOCKET = null;
	private boolean THREAD_EXIT = false;

	private final Log LOG = LogFactory.getLog(DataConnectionHandler.class);
	
	public DataConnectionHandler(Socket socket, DataServer dataServer) {
		SOCKET = socket;
		DATASERVER = dataServer;
	}

	public void handleConnection() {
		Thread t = new Thread(this);
		t.start();
		LOG.info("SUCCESS: On "+(new Date().toString())+", a DataServer connection has been made.");
	}

	//THIS HANDLES ONE STATEMENT -- AND ONLY ONE STATEMENT!
	@Override
	public void run() {
		ObjectOutputStream first = null;
		BufferedReader br_in = null;
		PrintWriter out = null;
		try {
			first = new ObjectOutputStream(SOCKET.getOutputStream());
			br_in = new BufferedReader(new InputStreamReader(SOCKET.getInputStream()));
			out = new PrintWriter(SOCKET.getOutputStream(),true);
		} catch (IOException e) {
			LOG.error("FATAL: Could not get input stream from Socket.");
		}
		String inputLine = null;
		
		 //send the ServerInfo (let them know a DataServer is connecting)
	    try {
			first.writeObject(DATASERVER.getInfo());
		} catch (IOException e) {
			LOG.error("FATAL: Could not write ServerInfo from Socket.",e);
			System.exit(1);
		}
		
		try {
			while(!THREAD_EXIT) {
				while((inputLine = br_in.readLine()) != null) {
					if(inputLine.startsWith("write")) {
						DATASERVER.writeBlock(inputLine.substring(5).trim(), out);
					} else if(inputLine.startsWith("append")) {
						DATASERVER.appendBlock(inputLine.substring(6).trim(), out);
					} else if(inputLine.startsWith("create")) {
						DATASERVER.createFile(inputLine.substring(6).trim(), out);
					} else if(inputLine.startsWith("remove")) {
						DATASERVER.removeFile(inputLine.substring(6).trim(), out);
						THREAD_EXIT = true;
						stop();
						break;
					} else if(inputLine.startsWith("head")) {
						DATASERVER.readHeadFile(inputLine.substring(4).trim(), out);
						THREAD_EXIT = true;
						stop();
						break;
					} else if(inputLine.startsWith("tail")) {
						DATASERVER.readTailFile(inputLine.substring(4).trim(), out);
						THREAD_EXIT = true;
						stop();
						break;
					} else if(inputLine.startsWith("sysinfo")) {
						DATASERVER.updateSystemInfo();
						first.writeObject(DATASERVER.getInfo());
						THREAD_EXIT = true;
						stop();
						break;
					} else if(inputLine.equals("shutdown")) {
						THREAD_EXIT = true;
						DATASERVER.handleShutdown(out);
						stop();
						break;
					} else {
						LOG.info(inputLine);
					}
				}
			}
		} catch (IOException e) {
			LOG.error("FATAL: Could not read from the input stream. (MetaServer disconnect?)");
			stop();
		}
	}
	
	public void stop() {
        LOG.info("SUCCESS: On "+(new Date().toString())+", a DataServer connection has been closed.");
    }
}
