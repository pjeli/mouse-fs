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


public class MetaConnectionHandler implements Runnable {
	private MetaServer METASERVER = null;
	private Socket SOCKET = null;
	private boolean SHUTDOWN = false;

	private final Log LOG = LogFactory.getLog(MetaConnectionHandler.class);
	
	public MetaConnectionHandler(Socket socket, MetaServer metaServer) {
		SOCKET = socket;
		METASERVER = metaServer;
	}

	public void handleConnection() {
		Thread t = new Thread(this);
		t.start();
		LOG.info("SUCCESS: On "+(new Date().toString())+", a MetaServer connection has been made.");
	}

	//THIS HANDLES ONE STATEMENT -- AND ONLY ONE STATEMENT!
	@Override
	public void run() {
		ObjectInputStream first = null;
		BufferedReader br = null;
		PrintWriter out = null;
		try {
			first = new ObjectInputStream(SOCKET.getInputStream());
			br = new BufferedReader(new InputStreamReader(SOCKET.getInputStream()));
			out = new PrintWriter(new OutputStreamWriter(SOCKET.getOutputStream()), true);
		} catch (IOException e) {
			LOG.error("FATAL: Could not get input stream from Socket.");
		}
		String inputLine = null;
		ServerInfo serverInfo = null;
		
		try {
			serverInfo = (ServerInfo) first.readObject();
		} catch (ClassNotFoundException | IOException e) {
			LOG.error("FATAL: Could not read the ServerInfo object.",e);
		}
		
		/* LOOP #1 -- PARSING CLI COMMANDS */
		/* LOOP #1 -- PARSING CLI COMMANDS */
		/* LOOP #1 -- PARSING CLI COMMANDS */
		
		if(serverInfo.getType() == ServerInfo.TYPES.CLI) {
			LOG.info("SUCCESS: Received CLI connection.");
			METASERVER.addCLI(serverInfo);
			try {
				while(!SHUTDOWN) {
					while(br.ready()) {
						inputLine = br.readLine();
						if(inputLine.equals("save")) {
							METASERVER.saveJournal(out);
						} else if(inputLine.startsWith("mkdirs")) {
							METASERVER.mkdirs(inputLine.substring(6).trim(), out);
						} else if(inputLine.startsWith("mkdir")) {
							METASERVER.mkdir(inputLine.substring(5).trim(), out);
						} else if(inputLine.startsWith("rmdir")) {
							METASERVER.rmdir(inputLine.substring(5).trim(), out);
						} else if(inputLine.startsWith("create")) {
							METASERVER.createFile(inputLine.substring(6).trim(), out);
						} else if(inputLine.startsWith("write")) {
							METASERVER.writeIntoFile(inputLine.substring(5).trim(), out);
						} else if(inputLine.startsWith("append")) {
							METASERVER.appendIntoFile(inputLine.substring(6).trim(), out);
						} else if(inputLine.startsWith("head")) {
							METASERVER.readFile(inputLine.substring(4).trim(), out, DataWriter.ACTIONS.HEAD);
						} else if(inputLine.startsWith("tail")) {
							METASERVER.readFile(inputLine.substring(4).trim(), out, DataWriter.ACTIONS.TAIL);
						} else if(inputLine.startsWith("list")) {
							METASERVER.list(inputLine.substring(4).trim(), out);
						} else if(inputLine.startsWith("sysinfo")) {
							METASERVER.printSystemInfo(out);
						} else if(inputLine.equals("format")) {
							METASERVER.format(out);
						} else if(inputLine.equals("help")) {
							METASERVER.sendHelp(out);
						} else if(inputLine.equals("logout")) {
							SHUTDOWN = true;
							METASERVER.logout();
							stop();
							break;
						} else if(inputLine.equals("shutdown")) {
							SHUTDOWN = true;
							METASERVER.handleShutdown(out);
							stop();
							break;
						} else {
							out.println("Received unknown command := "+inputLine+".");
							LOG.error("Received unknown command := "+inputLine+".");
						}
					}
				}
			} catch (IOException e) {
				LOG.error("FATAL: Could not read from the input stream. (CLI disconnect?)",e);
				stop();
			}
			
		/* LOOP #2 -- PARSING DATASERVER COMMANDS */
		/* LOOP #2 -- PARSING DATASERVER COMMANDS */
		/* LOOP #2 -- PARSING DATASERVER COMMANDS */
		
		} else if(serverInfo.getType() == ServerInfo.TYPES.DATA) {
			LOG.info("SUCCESS: Received DataServer connection.");
			METASERVER.addDataServer((DataServerInfo) serverInfo);
			try {
				while(!SHUTDOWN) {
					while(br.ready()) {
						inputLine = br.readLine();
						if(inputLine.startsWith("heartbeat")) {
							out.println("Heartbeat was received.");
							LOG.info("SUCCESS: Received heartbeat from "+inputLine.substring(9).trim()+".");;
						} else {
							out.println("Received unknown command := "+inputLine+".");
							LOG.error("Received unknown command := "+inputLine+".");
						}
					}
				}
			} catch (IOException e) {
				LOG.error("FATAL: Could not read from the input stream. (DataServer disconnect?)",e);
				stop();
			}
		} else {
			LOG.error("FATAL: Could not tell what was connecting to MetaServer. Closing connection.");
			stop();
		}
	}
	
	public void stop() {
		LOG.info("SUCCESS: On "+(new Date().toString())+", a MetaServer connection has been closed.");
	}
}

