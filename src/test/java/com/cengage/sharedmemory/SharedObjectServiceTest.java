package com.cengage.sharedmemory;

import static org.junit.Assert.*;

import org.junit.Test;

public class SharedObjectServiceTest {

	@Test
	public void canAcceptAnObjectToStore() throws Exception {
		SharedObjectService service = new SharedObjectService(System.getProperty("java.io.tmpdir"), "test");
		
		Object value = service.put("key", "value");
		
		assertNotNull(value);
	}

}
