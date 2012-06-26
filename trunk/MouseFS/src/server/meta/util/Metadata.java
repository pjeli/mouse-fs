package server.meta.util;

import java.io.Serializable;

import server.info.ServerInfo;

/**
 * THIS CLASS REPRESENTS ONE BLOCK FOR A SPECIFIED FILENAME.
 * THIS "BLOCK" CAN EXIST IN MULTIPLE DATASERVERS THEREFORE
 * THE SERVERINFOS PER BLOCK ARE STORED
 * @author Plamen Jeliazkov
 *
 */
public class Metadata implements Serializable {
	/**
	 * 
	 */
	public static final long serialVersionUID = 4195491722922744420L;
	private String filename;
	private String date_created;
	private String date_modified;
	private String permissions;
	private String owner;
	private int blockCount;
	
	//contains information regarding the server this file is stored on
	private ServerInfo[] dataServer;
	
	public Metadata(String filename, String date_created, String date_modified, 
			String permissions, String owner, ServerInfo[] dataServer) {
		this.setFilename(filename);
		this.setPermissions(permissions);
		this.setDate_created(date_created);
		this.setDate_modified(date_modified);
		this.setOwner(owner);
		this.setBlockCount(0);
		this.setDataServer(dataServer);
	}
	
	public String getFilename() {
		return filename;
	}
	private void setFilename(String filename) {
		this.filename = filename;
	}
	public String getDate_created() {
		return date_created;
	}
	private void setDate_created(String date_created) {
		this.date_created = date_created;
	}
	public String getDate_modified() {
		return date_modified;
	}
	private void setDate_modified(String date_modified) {
		this.date_modified = date_modified;
	}
	public String getPermissions() {
		return permissions;
	}
	private void setPermissions(String permissions) {
		this.permissions = permissions;
	}
	public String getOwner() {
		return owner;
	}
	private void setOwner(String owner) {
		this.owner = owner;
	}
	public String toString() {
		return filename;
	}

	int getBlockCount() {
		return blockCount;
	}

	void setBlockCount(int blockCount) {
		this.blockCount = blockCount;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(getFilename().equals(obj.toString())) {
			return true;
		}
		return false;
	}

	public ServerInfo[] getDataServer() {
		return dataServer;
	}

	public void setDataServer(ServerInfo[] dataServer) {
		this.dataServer = dataServer;
	}
}
