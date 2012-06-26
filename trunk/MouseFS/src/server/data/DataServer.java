package server.data;

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
import java.net.ConnectException;
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

import server.data.util.Block;
import server.info.DataServerInfo;
import server.info.ServerInfo;


public class DataServer {
	public static final Log LOG = LogFactory.getLog(DataServer.class);
	private final boolean DEBUG = true;

	private ConcurrentHashMap<String, ArrayList<Block>> CACHE = null;
	private final Date START_TIME = new Date();
	private String IP_ADDRESS;
	private String REMOTE_ADDRESS;
	private static final int  META_PORT = 7400;
	private int DATA_PORT = 0;
	private String JOURNAL_PATH;
	private String IDDIR;
	private String WORKDIR;
	private String FINALDIR;
	private Socket CLIENT = null;
	private ServerSocket SERVER = null;
	private DataServerInfo INFO = null;
	private long ID = 0;
	
	//System information fields
	private int CORES;
	private long CURR_MEMORY;
	private long MAX_MEMORY;
	private long JVM_MEMORY;
	private long FREE_SPACE;
	
	public DataServer(String address, int port, String data_path) throws ConnectException {
		if(!data_path.endsWith("/")) {
			LOG.error("FATAL: Data path does not end with a '/'.");
			System.exit(1);
		}
		this.REMOTE_ADDRESS = address;
		this.DATA_PORT = port;
		this.JOURNAL_PATH = data_path+"data_journal.ser";
		this.IDDIR = data_path+"data_server.id";
		this.WORKDIR = data_path+"tmp/";
		this.FINALDIR = data_path+"finalized/";
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

		//connect to local or remote metaserver socket
		if(REMOTE_ADDRESS != null) {
			try {
				CLIENT = new Socket(getRemoteAddress(),META_PORT);
			} catch (IOException e2) {
				LOG.error("FATAL: Could not connect socket.",e2);
				System.exit(1);
			}
		} else {
			try {
				CLIENT = new Socket(getAddress(),META_PORT);
			} catch (IOException e1) {
				
				LOG.error("FATAL: Could not connect socket.",e1);
				System.exit(1);
			}
		}
		
		//try to read cache (or get new)
		File journal = new File(JOURNAL_PATH);
		if(journal.exists()) {
			readJournal();
			LOG.info("SUCCESS: Cache has been read.");
		}
		else {
			CACHE = new ConcurrentHashMap<String, ArrayList<Block>>(5000);
			LOG.info("SUCCESS: Cache was newly initialized.");
		}
		
		INFO = new DataServerInfo(getAddress(), getPort(), getID(), ServerInfo.TYPES.DATA);
		getSystemInfo();
		
		//connect server socket
		try {
			SERVER = new ServerSocket(getPort());
		} catch (IOException e) {
			LOG.error("FATAL: Could not connect ServerSocket.",e);
			System.exit(1);
		}
		
		LOG.info("SUCCESS: On "+getStartTime()+", "+getInfo()+" is active.");
		
		handleConnections();
	}

	public void handleShutdown(PrintWriter out) {
		saveJournal(out);
		
		LOG.info("SUCCESS: Shutting down DataServer in 1 minute...");

		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
			LOG.error("FATAL: Could not sleep for 60 seconds.");
		}
		
		LOG.info("DATASERVER SHUTDOWN!");
		System.exit(1);
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
				LOG.error("Could not create ID file.", e);
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

	private void createWorkingDirectory() {
		File workdir = new File(WORKDIR);
		if(!workdir.exists()) {
			//make parent directory and file
			workdir.mkdirs();
		}
		
		File finaldir = new File(FINALDIR);
		if(!finaldir.exists()) {
			//make parent directory and file
			finaldir.mkdirs();
		}
	}
	
	/* Should only need to handle 1 MetaServer connection at a time. */
	private void handleConnections() {
		
		//spawn 1 connection between DataServer and MetaServer
		DataConnectionHandler handler = new DataConnectionHandler(CLIENT, this);
		handler.handleConnection();
		
		//continually listen for incoming connections (possibly CLI or DataWriters)
		while (true) {
			Socket socket = null; //this socket is a connecting local metaserver
			try {
				socket = SERVER.accept(); //blocking call here until connection occurs
			} catch (IOException e) {
				LOG.error("FATAL: Could not accept the socket connection.",e);
				return;
			}
			DataConnectionHandler otherHandlers = new DataConnectionHandler(socket, this);
			otherHandlers.handleConnection();
		}
	}
	
	//path = FULL PATH aka key, out = output to send back to MetaServer
	public void createFile(String path, PrintWriter out) {
		if(CACHE.containsKey(path)) {
			out.println("Block listing already exists for "+path+".");
			LOG.error("FATAL: Tried to create new Blocks for "+path+". (MetaServer not synced with others?)");
		}
		
		Block block = new Block();
		File blockFile = new File(WORKDIR+block.getFileName());
		try {
			blockFile.createNewFile();
		} catch (IOException e) {
			out.println("Could not create a Block for "+path+".");
			LOG.error("Could not create a Block for "+path+".", e);
		}
		
		ArrayList<Block> blockList = new ArrayList<Block>(3);
		blockList.add(block);
		CACHE.put(path, blockList);
		
		LOG.info("SUCCESS: Block listing created for "+path+".");
		
		saveJournal(out);
	}
	
	//THIS WILL OVERWRITE DATA IN BLOCK!
	public void writeBlock(String input, PrintWriter out) {
		String[] parsed = input.split(" ",2);
		
		if(parsed.length != 2) {
			out.println("Please put a space between the filepath and the data you wish to write.");
			LOG.error("FATAL: Recieved input was not splittable into a filepath and data.");
			return;
		}
		
		String path = parsed[0];
		
		//need a lot of escaping here
		String data = parsed[1].replace("\\n", "\n");
		
		ArrayList<Block> blocks = CACHE.get(path);
		
		if(blocks == null || blocks.size() == 0) {
			out.println("There are no blocks available for "+path+" (File does not exist?).");
			LOG.error("FATAL: There are no blocks available for "+path+" (File does not exist?).");
			return;
		}
		
		//get the latest block
		Block block = blocks.get(blocks.size()-1);
		
		File rawFile = new File(WORKDIR+block.getFileName());
		try {
			FileWriter fw = new FileWriter(rawFile);
			fw.write(data);
			fw.flush();
			fw.close();
		} catch (IOException e) {
			out.println("Issue writing into block file "+block+".");
			LOG.error("FATAL: Issue writing into block file "+block+".");
		}
		
		out.println("Data was successfully written for "+path+" into block file "+block+".");
		LOG.info("SUCCESS: Data was successfully written for "+path+" into block file "+block+".");
	}
	
	//THIS WILL APPEND DATA IN BLOCK!
	public void appendBlock(String input, PrintWriter out) {
		String[] parsed = input.split(" ",2);
		
		if(parsed.length != 2) {
			out.println("Please put a space between the filepath and the data you wish to write.");
			LOG.error("FATAL: Recieved input was not splittable into a filepath and data.");
			return;
		}
		
		String path = parsed[0];
		
		//need a lot of escaping here
		String data = parsed[1].replace("\\n", "\n");
		
		ArrayList<Block> blocks = CACHE.get(path);
		
		if(blocks == null || blocks.size() == 0) {
			out.println("There are no blocks available for "+path+" (File does not exist?).");
			LOG.error("FATAL: There are no blocks available for "+path+" (File does not exist?).");
			return;
		}
		
		//get the latest block
		Block block = blocks.get(blocks.size()-1);
		
		File rawFile = new File(WORKDIR+block.getFileName());
		try {
			FileWriter fw = new FileWriter(rawFile, true);
			fw.write(data);
			fw.flush();
			fw.close();
		} catch (IOException e) {
			out.println("Issue writing into block file "+block+".");
			LOG.error("FATAL: Issue writing into block file "+block+".");
		}
		
		out.println("Data was successfully written for "+path+" into block file "+block+".");
		LOG.info("SUCCESS: Data was successfully written for "+path+" into block file "+block+".");
	}

	/**
	 * Generates an automatic data server using port 7500 and data path "./data".
	 */
	public static void main(String[] args) {
		try {
			if(args.length > 0) 
				new DataServer(args[0], 7500, "./data/");
			else
				new DataServer(null, 7500, "./data/");
		} catch(ConnectException ex) {
			System.out.println("FATAL: Could not connect socket. No MetaServer to connect to.");
		}
	}
	
	@SuppressWarnings("unchecked")
	private void readJournal() {
		File journal = new File(JOURNAL_PATH);
		try {
			FileInputStream fis = new FileInputStream(journal);
			ObjectInputStream obj = new ObjectInputStream(fis);
			CACHE = (ConcurrentHashMap<String, ArrayList<Block>>) obj.readObject();
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
	
	private void getSystemInfo() {
	    CORES = Runtime.getRuntime().availableProcessors();
	    CURR_MEMORY = Runtime.getRuntime().freeMemory();
	    MAX_MEMORY = Runtime.getRuntime().maxMemory();
	    JVM_MEMORY = Runtime.getRuntime().totalMemory();
	    File root = new File("C:/");
		FREE_SPACE = root.getFreeSpace();

		//update the DataServerInfo with the new stats
		INFO.setCores(CORES);
		INFO.setCurrMemory(CURR_MEMORY);
		INFO.setFreeSpace(FREE_SPACE);
		INFO.setJVMMemory(JVM_MEMORY);
		INFO.setMaxMemory(MAX_MEMORY);
	}
	
	public void updateSystemInfo() {
		getSystemInfo();
		LOG.info("System information updated.");
	}
	
	public String getAddress() {
		return IP_ADDRESS;
	}
	
	public String getRemoteAddress() {
		return REMOTE_ADDRESS;
	}

	public int getPort() {
		return DATA_PORT;
	}

	public Date getStartTime() {
		return START_TIME;
	}
	
	public long getID() {
		return ID;
	}
	
	public ServerInfo getInfo() {
		return INFO;
	}

	/**
	 * Only sends 1 Megabyte (head) of the file asked to read.
	 * @param path
	 * @param out
	 */
	public void readHeadFile(String path, PrintWriter out) {
		ArrayList<Block> blocks = CACHE.get(path);
	
		if(blocks == null || blocks.size() == 0) {
			out.println("There are no blocks available for "+path+" (File does not exist?).");
			LOG.error("FATAL: There are no blocks available for "+path+" (File does not exist?).");
			return;
		}
		
		//ONLY READ THE FIRST 1 MB OF THE FILE
		
		//READ THE FIRST BLOCK	
		BufferedReader br = null;
		int chars_read = 0;
		try {
			br = new BufferedReader(new FileReader(WORKDIR+blocks.get(0).toString()));
			StringBuilder output = new StringBuilder();
			while(br.ready() && chars_read < 1024) {
				output.append((char) br.read());
				chars_read++;
			}
			br.close();
			out.println(output.toString());
		} catch (IOException e) {
			out.println("We could not read the Block file.");
			LOG.error("FATAL: We could not read the Block file.",e);
		}
	}
	
	/**
	 * Only sends 1 Megabyte (tail) of the file asked to read.
	 * @param path
	 * @param out
	 */
	public void readTailFile(String path, PrintWriter out) {
		ArrayList<Block> blocks = CACHE.get(path);
	
		if(blocks == null || blocks.size() == 0) {
			out.println("There are no blocks available for "+path+" (File does not exist?).");
			LOG.error("FATAL: There are no blocks available for "+path+" (File does not exist?).");
			return;
		}
		
		//ONLY READ THE LAST 1 MB OF THE FILE
		
		//READ THE LAST BLOCK	
		BufferedReader br = null;
		int chars_read = 0;
		
		long filesize = new File(WORKDIR+blocks.get(blocks.size()-1).toString()).length();
		long skip = 0;
		if(filesize > 1024) {
			skip = filesize - 1024;
		}
		
		try {
			br = new BufferedReader(new FileReader(WORKDIR+blocks.get(blocks.size()-1).toString()));
			if(skip != 0) {
				br.skip(skip);
			}
			
			StringBuilder output = new StringBuilder();
			while(br.ready() && chars_read < 1024) {
				output.append((char) br.read());
				chars_read++;
			}
			br.close();
			out.println(output.toString());
		} catch (IOException e) {
			out.println("We could not read the Block file.");
			LOG.error("FATAL: We could not read the Block file.",e);
		}
	}

	public void removeFile(String path, PrintWriter out) {
		ArrayList<Block> blocks = CACHE.get(path);
		
		if(blocks == null || blocks.size() == 0) {
			out.println("There are no blocks available for "+path+" (File does not exist?).");
			LOG.error("FATAL: There are no blocks available for "+path+" (File does not exist?).");
			return;
		}
		
		//DELETE THE BLOCKS
		for(Block block : blocks) {
			File file = new File(WORKDIR+block.getFileName());
			file.delete();
		}
		
		out.println("The file "+path+" was successfully deleted.");
		LOG.info("SUCCESS: The file "+path+" was successfully deleted.");
	}
}
