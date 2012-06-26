package server.data.util;

import java.io.Serializable;
import java.util.Random;

/**
 * Blocks are identified pseudo-randomly assigned long values.
 * @author Plamen Jeliazkov
 *
 */
public class Block implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2372449899986742033L;
	private final long BLOCKID = new Random().nextLong();
	private final static String PREFIX = "blk_";
	private boolean SATURATED = false;
	
	public Block() {}
	
	public String getFileName() {
		return PREFIX+BLOCKID;
	}
	
	public long getID() {
		return BLOCKID;
	}

	public boolean isSaturated() {
		return SATURATED;
	}

	public void setSaturated(boolean saturated) {
		SATURATED = saturated;
	}
	
	public String toString() {
		return getFileName();
	}
}
