package server.meta.util;

public class Metapath {
	private String path;
	
	public Metapath(String path) {
		this.path = path;
	}
	
	public Metapath getParent() {
		String[] nodes = path.split("/");
		
		if(nodes.length <= 1) {
			return null;
		}
		
		String parentPath = "";
		for(int i = 0; i < nodes.length-1; i++) {
			if(i == nodes.length-2) {
				parentPath += nodes[i];
			} else {
				parentPath += nodes[i] + "/";
			}
		}
		
		return new Metapath(parentPath);
	}
	
	public Metapath getEnd() {
		String[] nodes = path.split("/");
		
		if(nodes.length <= 1) {
			return null;
		}
		
		return new Metapath(nodes[nodes.length-1]);
	}
	
	public String toString() {
		return path;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(path.equals(obj.toString())) {
			return true;
		}
		return false;
	}
}
