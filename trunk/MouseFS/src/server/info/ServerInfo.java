package server.info;

import java.io.Serializable;

public class ServerInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1995979098294496243L;
	
	private String IP_ADDRESS;
	private int PORT;
	private long ID;
	private TYPES TYPE;
	public static enum TYPES {META, DATA, CLI};
	
	public ServerInfo(String address, int port, long id, TYPES type) {
		this.IP_ADDRESS = address;
		this.PORT = port;
		this.ID = id;
		this.TYPE = type;
	}
	
	public String getAddress() {
		return IP_ADDRESS;
	}
	
	public int getPort() {
		return PORT;
	}
	
	public long getID() {
		return ID;
	}
	
	public TYPES getType() {
		return TYPE;
	}
	
	public String toString() {
		String type = null;
		switch(TYPE) {
		case CLI: type = "CLI"; break;
		case DATA: type = "DataServer"; break;
		case META: type = "MetaServer"; break;
		default: type = "Error"; break;
		}
		
		return type+"@"+getAddress()+":"+getPort()+":ID::"+getID();
	}
}
