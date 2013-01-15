package com.cengage.sharedmemory;

import static org.junit.Assert.*;

import org.junit.Test;

public class SharedObjectMapTest {

	@Test
	public void canAcceptAnObjectToStore() throws Exception {
		SharedObjectMap service = new SharedObjectMap(System.getProperty("java.io.tmpdir"), "test");
		
		Object value = service.put("key", "value");
		
		assertNotNull(value);
	}

}
