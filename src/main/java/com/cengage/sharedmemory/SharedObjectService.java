package com.cengage.sharedmemory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class SharedObjectService implements Map {
	private static final String UNIMPLEMENTED_METHOD = "Unimplemented Method";

	private static Logger LOG = LoggerFactory.getLogger(SharedObjectService.class);

	private BigFile bigFile;
	protected Map<String, BigFile.Location>objectLocations;
	
	public SharedObjectService(String fileLocation, String filePrefix) throws IOException {	
		LOG.info("Creating shared object repository with prefix " + filePrefix + " in " + fileLocation);
	       
		objectLocations = new HashMap<String, BigFile.Location>();
		try {
			bigFile = new BigFile(Integer.MAX_VALUE / 8, fileLocation, filePrefix);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }

	@Override
	public Object put(Object key, Object value) {
		LOG.debug("Adding key :" + key);

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);   
			out.writeObject(value);
			BigFile.Location location = null;
			if (objectLocations.containsKey(key)) {
				location = objectLocations.get(key);
				bigFile.update(location, bos.toByteArray());
			} else {
				location = bigFile.add(bos.toByteArray());
			}
				
			objectLocations.put((String) key, location);
		} catch (IOException e) {
			LOG.error("Error putting key:" + key, e);
		} finally {
			try {
				out.close();
				bos.close();
			} catch (Exception e) {}
		}
		return value;
	}
	
	@Override
	public boolean containsKey(Object key) {
		return objectLocations.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		throw new RuntimeException(UNIMPLEMENTED_METHOD);
	}

	@Override
	public Object get(Object key) {
		Object value = null;
		BigFile.Location location = objectLocations.get(key);
		if (location != null) {
			byte[] data = bigFile.get(location);
			
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			ObjectInput in = null;
			try {
			  in = new ObjectInputStream(bis);
			  value = in.readObject(); 
			} catch (IOException | ClassNotFoundException e) {
				LOG.error("Error getting Key:" + key, e);
			} finally {
				try {
					in.close();
					bis.close();
				} catch (Exception e) {}
			} 
		}
		return value;
	}
	
	@Override
	public void clear() {
		bigFile.close();
	}

	@Override
	public int size() {
		return objectLocations.size();
	}

	@Override
	public Set entrySet() {
		throw new RuntimeException(UNIMPLEMENTED_METHOD);
	}

	@Override
	public boolean isEmpty() {
		throw new RuntimeException(UNIMPLEMENTED_METHOD);
	}

	@Override
	public Set keySet() {
		throw new RuntimeException(UNIMPLEMENTED_METHOD);	
	}

	@Override
	public void putAll(Map map) {
		throw new RuntimeException(UNIMPLEMENTED_METHOD);
	}

	@Override
	public Object remove(Object key) {
		throw new RuntimeException(UNIMPLEMENTED_METHOD);
	}

	@Override
	public Collection values() {
		throw new RuntimeException(UNIMPLEMENTED_METHOD);
	}
}