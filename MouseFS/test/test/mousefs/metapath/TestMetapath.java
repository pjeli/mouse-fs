package test.mousefs.metapath;

import static org.junit.Assert.*;

import org.junit.Test;

import server.meta.util.Metapath;

public class TestMetapath {

	@Test
	public void test() {
		Metapath a = new Metapath("/");
		Metapath b = new Metapath("test");
		Metapath c = new Metapath("test/lewis");
		Metapath x = new Metapath("test/lewis/hard/work/porn/plamen");
		Metapath p = new Metapath("");
		Metapath q = p.getParent();
		Metapath r = a.getParent();
		Metapath s = b.getParent();
		Metapath t = c.getParent();
		Metapath y = x.getParent();
		Metapath z = y.getParent();
		
		assertNull(q);
		assertNull(r);
		assertNull(s);
		assertTrue(t.toString().equals("test"));
		assertTrue(y.toString().equals("test/lewis/hard/work/porn"));
		assertTrue(z.toString().equals("test/lewis/hard/work"));
	}
	
}
