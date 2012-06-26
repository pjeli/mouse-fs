package server.meta;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import server.info.DataServerInfo;
import server.info.ServerInfo;
import server.meta.util.Metadata;
import server.meta.util.Metapath;


public class MetaServer {
	private final Log LOG = LogFactory.getLog(MetaServer.class);
	private final boolean DEBUG = true;
	
	//Has to be a concurrent Map?
	private ConcurrentHashMap<String, ArrayList<Metadata>> CACHE = null;
	private ArrayList<DataServerInfo> DATASERVERS = new ArrayList<DataServerInfo>(10);
	private ServerInfo CLI = null;
	
	private final String OWNER = System.getProperty("user.name");
	private final Date START_TIME = new Date();
	private String JOURNAL_PATH;
	private String IDDIR;
	private String META_PATH;
	private int META_PORT = 0;
	private ServerInfo INFO = null;
	private String IP_ADDRESS = null;
	private ServerSocket SERVER = null;
	private long ID = 0;
	
	//System information fields
	private int CORES;
	private long CURR_MEMORY;
	private long MAX_MEMORY;
	private long JVM_MEMORY;
	private long FREE_SPACE;
	private int UNIQUE_CONNECTS;
	
	/**
	 * Should be a nameNode style server that keeps all Metadata in DRAM.
	 * Journal values are written to a file on disk once the server is shutting down.
	 */
	public static void main(String[] args) {
		new MetaServer(7400, "./meta/");
	}
	
	public MetaServer(int port, String data_path) {
		if(!data_path.endsWith("/")) {
			LOG.error("FATAL: Data path does not end with a '/'.");
			System.exit(1);
		}
		
		this.META_PORT = port;
		this.JOURNAL_PATH = data_path+"meta_journal.ser";
		this.IDDIR = data_path+"meta_server.id";
		this.META_PATH = data_path;
		start();
	}

	private void start() {
		createWorkingDirectory();
		generateOrLoadID();
		
		//get IP address
		try {
			IP_ADDRESS = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			LOG.error("FATAL: Could not detect IP address:",e);
			System.exit(1);
		}
		
		//try to read cache (or get new)
		File journal = new File(JOURNAL_PATH);
		if(journal.exists()) {
			readJournal();
			LOG.info("SUCCESS: Cache has been read.");
		}
		else {
			CACHE = new ConcurrentHashMap<String, ArrayList<Metadata>>(5000);
			LOG.info("SUCCESS: Cache was newly initialized.");
		}
		
		initMetaServerInfo();
		
		
		//connect server socket
		try {
			SERVER = new ServerSocket(META_PORT);
		} catch (IOException e) {
			LOG.error("FATAL: Could not connect ServerSocket.",e);
			System.exit(1);
		}
		
		//accept the socket
		LOG.info("SUCCESS: On "+getStartTime()+", "+getInfo()+" is active.");
		handleConnections();
	}

	private void initMetaServerInfo() {
		getSystemInfo(false);
		INFO = new ServerInfo(getAddress(), getPort(), getID(), ServerInfo.TYPES.META);
	}
	
	private void handleConnections() {
		LOG.info("Waiting for incoming connections...");
		
		while (true) {
			Socket socket = null; //this socket is a connecting local dataserver
			try {
				socket = SERVER.accept(); //blocking call here until connection occurs
			} catch (IOException e) {
				LOG.error("FATAL: Could not accept the socket connection. (Shutting down?)",e);
				return;
			}
			MetaConnectionHandler handler = new MetaConnectionHandler(socket, this);
			handler.handleConnection();
		}
	}
	
	private void createWorkingDirectory() {
		File workdir = new File(META_PATH);
		if(!workdir.exists()) {
			//make parent directory and file
			workdir.mkdirs();
		}
	}
	
	private void generateOrLoadID() {
		File id = new File(IDDIR);
		if(!id.exists()) {
			ID = new Random().nextLong();
			try {
				id.createNewFile();
				BufferedWriter bw = new BufferedWriter(new FileWriter(id));
				bw.write(Long.toString(ID));
				bw.close();
				LOG.info("Could not find ID file. New server ID generated @"+id.getAbsolutePath()+".");
			} catch (IOException e) {
				LOG.error("FATAL: Could not create ID file: "+id.getAbsolutePath(), e);
				System.exit(1);
			}
			return;
		}
		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(id));
			ID = Long.parseLong(br.readLine());
		} catch (NumberFormatException | IOException e) {
			LOG.error("Could not open ID file for reading.", e);
			System.exit(1);
		}
	}
	
	public void updateDataServer(DataServerInfo dataServerUpdate) {
		for(DataServerInfo dataServerInfo : DATASERVERS) {
			if(dataServerInfo.equals(dataServerUpdate)) {
				int index = DATASERVERS.indexOf(dataServerInfo);
				DATASERVERS.set(index, dataServerUpdate);
				LOG.info("Updated DataServerInfo: "+dataServerUpdate);
			}
		}
	}
	
	public void addDataServer(DataServerInfo serverInfo) {
		DATASERVERS.add(serverInfo);
		LOG.info("Added new DataServer := "+serverInfo);
	}
	
	public void addCLI(ServerInfo serverInfo) {
		if(CLI == null) {
			CLI = serverInfo;
			LOG.info("Added new CLI := "+serverInfo);
		} else {
			LOG.error("FATAL: Another CLI tried to connect := "+serverInfo);
		}
	}
	
	public void format(PrintWriter out) {
		File journal = new File(JOURNAL_PATH);
		if(journal.exists()) {
			journal.delete();
		}
		CACHE.clear();
		out.println("The MetaServer has been formatted.");
		LOG.info("SUCCESS: The MetaServer has been formatted.");
	}
	
	public void saveJournal(PrintWriter out) {
		File journal = new File(JOURNAL_PATH);
		
		if(!journal.exists()) {
			try {
				journal.createNewFile();
			} catch (IOException e) {
				out.println("FATAL: Could not create the journal file.");
				LOG.error("FATAL: Could not create the journal file.",e);
			}
		}
		
		try {
			FileOutputStream fos = new FileOutputStream(journal, false);
			ObjectOutputStream outstream = new ObjectOutputStream(fos);
			outstream.writeObject(CACHE);
			outstream.flush();
			outstream.close();
			fos.close();
		} catch (IOException e) {
			out.println("FATAL: Could not write to the journal.");
			LOG.error("FATAL: Could not write to the journal.",e);
			System.exit(1);
		}
		out.println("The journal was written to disk @"+journal.getAbsolutePath());
		LOG.info("SUCCESS: The journal was written to disk @"+journal.getAbsolutePath());
	}
	
	/** 
	 * Will create the directories if they do not already exist in the CACHE.
	 * @param path
	 * @param out 
	 */
	public void mkdirs(String path, PrintWriter out) {
		if(path.endsWith("/") || path.startsWith("/")) {
			out.println("Do not end or start a file path with the '/' character.");
			LOG.error("Do not end or start a file path with the '/' character.");
			return;
		}
		
		Metapath p = new Metapath(path);
		
		if(CACHE.containsKey(path)) {
			out.println("Could not create directory "+path+" because it already exists.");
			LOG.error("Could not create directory "+path+" because it already exists.");
			return;
		}
		

		/* DIRECTORYS ARE ONLY STORED AS KEYS IN THE JOURNAL!! */
		/* DIRECTORYS ARE ONLY STORED AS KEYS IN THE JOURNAL!! */
		/* THIS LOOP ONLY MAKES THE PARENT DIRECTORYS!! */
		
		while(p.getParent() != null) { 
			p = p.getParent();
			String parent = p.toString();
			if(CACHE.containsKey(parent)) {
				break;
			} else {
				ArrayList<Metadata> array = new ArrayList<Metadata>(10);
				CACHE.put(parent, array);
				LOG.info("SUCCESS: New directory "+parent+" was created.");
			}
		}
		
		ArrayList<Metadata> array = new ArrayList<Metadata>(10);
		CACHE.put(path, array);
		out.println("New directory "+path+" was created.");
		LOG.info("SUCCESS: New directory "+path+" was created.");
	}
	
	/**
	 * This is a recursive removal of directory path.
	 * @param path
	 * @param out 
	 */
	public void rmdir(String path, PrintWriter out) {
		if(path.endsWith("/") || path.startsWith("/")) {
			out.println("Do not end or start a file path with the '/' character.");
			LOG.error("FATAL: Do not end or start a file path with the '/' character.");
			return;
		}
		
		for(String key : CACHE.keySet()) {
			if(key.startsWith(path)) {
				ArrayList<Metadata> metadata = CACHE.remove(key);
				for(Metadata meta : metadata) {
					Socket dataServerSocket = null;
					try {
						dataServerSocket = new Socket(meta.getDataServer()[0].getAddress(), meta.getDataServer()[0].getPort());
					} catch (IOException e) {
						LOG.info("FATAL: Could not create Socket for communicating with DataServer.");
					}
					DataWriter dw = new DataWriter(dataServerSocket, key+"/"+meta.getFilename(), null, DataWriter.ACTIONS.REMOVE, out, null);
					dw.handleAction();
				}
				out.println("Removed "+key+".");
			}
		}
		
		LOG.info("SUCCESS: Directory (and children files) "+path+" was deleted.");
	}
	
	public void list(String path, PrintWriter out) {
		if(CACHE.size() == 0) {
			out.println("There are no root directories. Make a new directory.");
			return;
		}
		
		if(path.endsWith("/") || path.startsWith("/")) {
			out.println("Do not end or start a file path with the '/' character.");
			LOG.error("FATAL: Do not end or start a file path with the '/' character.");
			return;
		}

		for(String key : CACHE.keySet()) {
			if(key.startsWith(path)) {
				out.println(key+" : "+CACHE.get(key));
			}
		}
	}
	
	public void mkdir(String path, PrintWriter out) {
		if(path.endsWith("/") || path.startsWith("/")) {
			out.println("Do not end or start a file path with the '/' character.");
			LOG.error("FATAL: Do not end or start a file path with the '/' character.");
			return;
		}
		
		Metapath p = new Metapath(path);
		
		if(CACHE.containsKey(path)) {
			out.println("Could not create directory "+path+" because it already exists.");
			LOG.error("FATAL: Could not create directory "+path+" because it already exists.");
			return;
		}
		
		if(p.getParent() != null) { 
			String parent = p.getParent().toString();
			if(!CACHE.containsKey(parent)) {
				out.println("Could not create directory because a parent does not exist.");
				LOG.error("FATAL: Could not create directory because a parent does not exist.");
				return;
			}
		}
		
		/* DIRECTORYS ARE ONLY STORED AS KEYS IN THE JOURNAL!! */
		/* DIRECTORYS ARE ONLY STORED AS KEYS IN THE JOURNAL!! */
		/* DIRECTORYS ARE ONLY STORED AS KEYS IN THE JOURNAL!! */
		
		ArrayList<Metadata> array = new ArrayList<Metadata>(10);
		CACHE.put(path, array);
		out.println("New directory "+path+" was created.");
		LOG.info("SUCCESS: New directory "+path+" was created.");
	}
	
	public void createFile(String path, PrintWriter out) {
		if(path.endsWith("/") || path.startsWith("/")) {
			out.println("Do not end or start a file path with the '/' character.");
			LOG.error("FATAL: Do not end or start a file path with the '/' character.");
			return;
		}
		
		Metapath p = new Metapath(path);
		
		if(p.getParent() != null) {
			String parent = p.getParent().toString();
			if(!CACHE.containsKey(parent)) {
				out.println("Could not create directory because a parent does not exist.");
				LOG.error("FATAL: Could not create directory because a parent does not exist.");
				return;
			}
		}
		
		Date now = new Date();
		ArrayList<Metadata> files = CACHE.get(p.getParent().toString());

		ServerInfo[] dataServer = pickDataServers(path);

		//save creation information into Metadata
		files.add(new Metadata(p.getEnd().toString(), now.toString(), now.toString(), "RW", getOwner(), dataServer));
		
		out.println("New file "+path+" was created in MetaServer.");
		LOG.info("SUCCESS: New file "+path+" was created in MetaServer.");
		
		for(ServerInfo serverInfo : dataServer) {
			Socket dataServerSocket = null;
			
			try {
				dataServerSocket = new Socket(serverInfo.getAddress(), serverInfo.getPort());
			} catch (IOException e) {
				LOG.info("FATAL: Could not create Socket for communicating with DataServer.");
			}
			
			//spawn action thread
			DataWriter dw = new DataWriter(dataServerSocket, path, null, DataWriter.ACTIONS.CREATE, out, null);
			dw.handleAction();
		}
	}
	
	public void writeIntoFile(String input, PrintWriter out) {
		String[] parsed = input.split(" ",2);
		
		if(parsed.length != 2) {
			out.println("Please put a space between the filepath and the data you wish to write.");
			LOG.error("FATAL: Recieved input was not splittable into a filepath and data.");
			return;
		}
		
		String path = parsed[0];
		String data = parsed[1];
		
		Metapath p = new Metapath(path);
		Metapath end = p.getEnd();
		String parent = p.getParent().toString();
		
		if(CACHE.containsKey(parent)) {
			ArrayList<Metadata> metadata = CACHE.get(parent);
			if(!metadata.contains(end)) {
				out.println("File does not exist in the MetaServer.");
				LOG.error("FATAL: File does not exist in the MetaServer.");
				return;
			}
		}
		
		if(DATASERVERS.size() < 1) {
			out.println("No DataServers are connected for writing.");
			LOG.error("FATAL: No DataServers are connected for writing.");
			return;
		}

		ServerInfo[] dataServer = fetchDataServers(path);

		for(ServerInfo serverInfo : dataServer) {
			Socket dataServerSocket = null;
			
			try {
				dataServerSocket = new Socket(serverInfo.getAddress(), serverInfo.getPort());
			} catch (IOException e) {
				LOG.info("FATAL: Could not create Socket for communicating with DataServer.");
			}
			
			//spawn action thread
			DataWriter dw = new DataWriter(dataServerSocket, path, data, DataWriter.ACTIONS.WRITE, out, null);
			dw.handleAction();
		}
	}

	public void appendIntoFile(String input, PrintWriter out) {
		String[] parsed = input.split(" ",2);
		
		if(parsed.length != 2) {
			out.println("Please put a space between the filepath and the data you wish to append.");
			LOG.error("FATAL: Recieved input was not splittable into a filepath and data.");
			return;
		}
		
		String path = parsed[0];
		String data = parsed[1];
		
		Metapath p = new Metapath(path);
		Metapath end = p.getEnd();
		Metapath parent = p.getParent();
		
		if(CACHE.containsKey(parent)) {
			ArrayList<Metadata> metadata = CACHE.get(parent.toString());
			if(!metadata.contains(end)) {
				out.println("File does not exist in the MetaServer.");
				LOG.error("FATAL: File does not exist in the MetaServer.");
				return;
			}
		}
		
		if(DATASERVERS.size() < 1) {
			out.println("No DataServers are connected for writing.");
			LOG.error("FATAL: No DataServers are connected for writing.");
			return;
		}

		ServerInfo[] dataServer = fetchDataServers(path);
		for(ServerInfo serverInfo : dataServer) {
			Socket dataServerSocket = null;
			
			try {
				dataServerSocket = new Socket(serverInfo.getAddress(), serverInfo.getPort());
			} catch (IOException e) {
				LOG.info("FATAL: Could not create Socket for communicating with DataServer.");
			}
			
			//spawn action thread
			DataWriter dw = new DataWriter(dataServerSocket, path, data, DataWriter.ACTIONS.APPEND, out, null);
			dw.handleAction();
		}
	}

	private ServerInfo[] fetchDataServers(String path) {
		Metapath p = new Metapath(path);
		Metapath parent = p.getParent();

		for(Metadata metadata : CACHE.get(parent.toString())) {
			if(metadata.equals(p.getEnd())) {
				return metadata.getDataServer();
			}
		}
		return null;
	}

	//TODO: fix this so it gets up to REPLICATION amount of servers.
	private ServerInfo[] pickDataServers(String path) {
		ServerInfo[] retval = new ServerInfo[1];
		int randomIndex = Math.abs(new Random().nextInt() % DATASERVERS.size());
		retval[0] = DATASERVERS.get(randomIndex);
		return retval;
	}
	
	public void readFile(String path, PrintWriter out, DataWriter.ACTIONS readType) {
		if(DATASERVERS.size() < 1) {
			out.println("No DataServers are connected for reading.");
			LOG.error("FATAL: No DataServers are connected for reading.");
			return;
		}
		
		Metapath p = new Metapath(path);
		Metapath end = p.getEnd();
		Metapath parent = p.getParent();
		
		if(CACHE.get(parent.toString()) != null) {
			ArrayList<Metadata> metadata = CACHE.get(parent.toString());
			if(!metadata.contains(end)) {
				out.println("File does not exist in the MetaServer.");
				LOG.error("FATAL: File does not exist in the MetaServer.");
				return;
			}
		}
		
		//In the case of reading we can just read from the first server
		ServerInfo[] dataServers = fetchDataServers(path);
		Socket dataServerSocket = null;
		for (ServerInfo dataServer : dataServers) {
			try {
				dataServerSocket = new Socket(dataServer.getAddress(), dataServer.getPort());
			} catch (IOException e) {
				out.println("Issue connecting to DataServer socket.");
				LOG.error("FATAL: Issue connecting to DataServer socket.",e);
			}
		}
		
		//spawn action thread
		DataWriter dw = new DataWriter(dataServerSocket, path, null, readType, out, null);
		dw.handleAction();
	}
	
	@SuppressWarnings("unchecked")
	private void readJournal() {
		File journal = new File(JOURNAL_PATH);
		try {
			FileInputStream fis = new FileInputStream(journal);
			ObjectInputStream obj = new ObjectInputStream(fis);
			CACHE = (ConcurrentHashMap<String, ArrayList<Metadata>>) obj.readObject();
			obj.close();
			fis.close();
		} catch (IOException e) {
			LOG.error("FATAL: We could not read the journal.",e);
			System.exit(1);
		} catch (ClassNotFoundException e) {
			LOG.error("FATAL: We could not decode the journal.",e);
			System.exit(1);
		}
		
		LOG.info("SUCCESS: Read in the journal! It has "+CACHE.size()+" entries in it.");
		if(DEBUG) {
			System.out.println(CACHE);
		}
	}
	
	//TODO: fix; currently only adds up MetaServers. Needs to be adding up DataServers.
	private void getSystemInfo(boolean all) {
		CORES = 0;
		CURR_MEMORY = 0;
		MAX_MEMORY = 0;
		JVM_MEMORY = 0;
		FREE_SPACE = 0;
		UNIQUE_CONNECTS = 0;
		
		if(all) {
			ArrayList<String> addresses = new ArrayList<String>();
			for(DataServerInfo dataServerInfo : DATASERVERS) {
				String dataServerAddress = dataServerInfo.getAddress();
				if(!addresses.contains(dataServerAddress)) {
					CORES += dataServerInfo.getCores();
					CURR_MEMORY += dataServerInfo.getCurrMemory();
					MAX_MEMORY += dataServerInfo.getMaxMemory();
					JVM_MEMORY += dataServerInfo.getJVMMemory();
					FREE_SPACE += dataServerInfo.getFreeSpace();
					UNIQUE_CONNECTS += 1;
					addresses.add(dataServerAddress);
				}
			}
		}
	}
	
	private void UpdateAllDataServerInfo(PrintWriter out) {
		for(DataServerInfo serverInfo : DATASERVERS) {
			Socket dataServerSocket = null;
			
			try {
				dataServerSocket = new Socket(serverInfo.getAddress(), serverInfo.getPort());
			} catch (IOException e) {
				LOG.info("FATAL: Could not create Socket for communicating with DataServer.");
			}
			
			//spawn action thread
			DataWriter dw = new DataWriter(dataServerSocket, null, null, DataWriter.ACTIONS.INFO, out, this);
			dw.handleAction();
		}
	}

	public void printSystemInfo(PrintWriter out) {
		UpdateAllDataServerInfo(out);
		getSystemInfo(true);

		out.println("Current connected system count: "+ (UNIQUE_CONNECTS));
		out.println("Current available cores: "+ CORES);
		out.println("Current available memory (MB): "+ (CURR_MEMORY / (1024*1024)));
		out.println("Current maximum memory (MB): "+ (MAX_MEMORY / (1024*1024)));
		out.println("Current JVM memory (MB): "+ (JVM_MEMORY / (1024*1024)));
		out.println("Current free disk space (MB): "+ (FREE_SPACE / (1024*1024)));
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

	public String getOwner() {
		return OWNER;
	}
	
	public long getID() {
		return ID;
	}
	
	public ServerInfo getInfo() {
		return INFO;
	}

	public void sendHelp(PrintWriter out) {
		out.println("The available commands are:");
		out.println("list\tlists all the directories and files");
		out.println("save\tsave the current namespace to disk");
		out.println("mkdirs\tmake an entire new directory structure");
		out.println("mkdir\tmake a single new directory");
		out.println("rmdir\trecursively remove a directory and files");
		out.println("create\tcreate a new file");
		out.println("write\twrite into a file (overwrites)");
		out.println("append\tappend into a file");
		out.println("head\tread the first Megabyte of a file");
		out.println("tail\tread the last Megabyte of a file");
		out.println("sysinfo\tget cluser information");
		out.println("format\tclear the namespace and journal");
		out.println("logout\tlogoff from command line interface");
		out.println("shutdown\tfull cluser shutdown");
	}

	public void handleShutdown(PrintWriter out) {
		saveJournal(out);

		for(ServerInfo dataServer : DATASERVERS) {
			Socket dataServerSocket = null;
			try {
				dataServerSocket = new Socket(dataServer.getAddress(), dataServer.getPort());
			} catch (IOException e) {
				LOG.info("FATAL: Could not create Socket for communicating with DataServer.");
			}
			
			//spawn action thread for shutting down DataServers
			DataWriter dw = new DataWriter(dataServerSocket, null, null, DataWriter.ACTIONS.SHUTDOWN, out, null);
			dw.handleAction();
		}

		LOG.info("SUCCESS: Shutting down MetaServer in 1 minute...");

		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
			LOG.error("FATAL: Could not sleep for 60 seconds.", e);
		}

		LOG.info("METASERVER SHUTDOWN!");
		System.exit(1);
	}

	public void logout() {
		CLI = null;	
		LOG.info("SUCCESS: Connected CLI has successfully logged out.");
	}
}

