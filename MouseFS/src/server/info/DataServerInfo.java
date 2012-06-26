package server.info;

import java.io.Serializable;

public class DataServerInfo extends ServerInfo implements Serializable {

	private int CORES;
	private long CURR_MEMORY;
	private long MAX_MEMORY;
	private long JVM_MEMORY;
	private long FREE_SPACE;
	/**
	 * 
	 */
	private static final long serialVersionUID = 8929146598156671470L;

	public DataServerInfo(String address, int port, long id, TYPES type) {
		super(address, port, id, type);
	}

	public int getCores() {
		return CORES;
	}

	public long getCurrMemory() {
		return CURR_MEMORY;
	}

	public long getMaxMemory() {
		return MAX_MEMORY;
	}

	public long getJVMMemory() {
		return JVM_MEMORY;
	}

	public long getFreeSpace() {
		return FREE_SPACE;
	}

	public void setCores(int cores) {
		CORES = cores;
	}

	public void setCurrMemory(long curr_memory) {
		CURR_MEMORY = curr_memory;
	}

	public void setMaxMemory(long max_memory) {
		MAX_MEMORY = max_memory;
	}

	public void setJVMMemory(long jvm_memory) {
		JVM_MEMORY = jvm_memory;
	}

	public void setFreeSpace(long free_space) {
		FREE_SPACE = free_space;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(toString().equals(obj.toString())) {
			return true;
		}
		return false;
	}
	
	@Override
	public String toString() {
		return getAddress()+":"+getPort();
	}
}
