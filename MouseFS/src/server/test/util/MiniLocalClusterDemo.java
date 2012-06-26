package server.test.util;

import java.net.ConnectException;
import java.util.Random;

import server.data.DataServer;
import server.meta.MetaServer;

public class MiniLocalClusterDemo {
	private static final int DATA_NUMBER = 3;
	
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		MetaServerThread mst = new MetaServerThread();

		for(int i = 0; i < DATA_NUMBER; i++) {
			DataServerThread dst = new DataServerThread();
		}
	}	
}

class DataServerThread implements Runnable {
	
	public DataServerThread () {
		Thread t = new Thread(this);
		t.start();
	}

	@Override
	public void run() {
		int port = Math.abs((new Random().nextInt() % 60000));
		try {
			new DataServer(null, port, "./data_"+port+"/");
		} catch (ConnectException e) {
			System.err.println("Failed to start DataServer.");
		}
	}	
}

class MetaServerThread implements Runnable {

	public MetaServerThread() {
		Thread t = new Thread(this);
		t.start();
	}

	@Override
	public void run() {
		int port = 7400;
		new MetaServer(port, "./meta_"+port+"/");
	}	
}