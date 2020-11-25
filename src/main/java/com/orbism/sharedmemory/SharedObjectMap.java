package com.orbism.sharedmemory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class SharedObjectMap implements Map {
	private static final String UNIMPLEMENTED_METHOD = "Unimplemented Method";
	private static final String KEY_FIELD = "key";

	private static Logger LOG = LoggerFactory.getLogger(SharedObjectMap.class);

	private BigFile bigFile;

	protected Directory vaultLocations;
	private IndexSearcher searcher;
	private IndexReader reader;
	private Set<String> fieldsToLoad = new HashSet<String>();
	private IndexWriter writer;
	private Analyzer standardAnalyzer;

	public SharedObjectMap(String fileLocation, String filePrefix) throws IOException {
		LOG.info("Creating shared object repository with prefix " + filePrefix + " in "
				+ fileLocation);

		try {
			bigFile = new BigFile(Integer.MAX_VALUE / 8, fileLocation, filePrefix);
			File path = new File(fileLocation + File.separator + "index");
			setupIndex(path);


			fieldsToLoad.add("file");
			fieldsToLoad.add("start");
			fieldsToLoad.add("size");
		} catch (IOException e) {
			LOG.error("Failed to intiialize map", e);
			throw new RuntimeException(e);
		}
	}

	private void setupIndex(File path) throws IOException {
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40, standardAnalyzer);
			  
		if ( path.exists() ) {
			vaultLocations = MMapDirectory.open(path);
			config.setOpenMode(OpenMode.CREATE_OR_APPEND);
		} else {
			vaultLocations = new MMapDirectory(path);
			config.setOpenMode(OpenMode.CREATE);
		}
		writer = new IndexWriter(vaultLocations, config);
		writer.commit();
		
		reader = DirectoryReader.open(vaultLocations);
		searcher = new IndexSearcher(reader);
	}

	@Override
	public Object put(Object key, Object value) {
		LOG.debug("Adding key :" + key);

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			if (locationExists(key)) {
				deleteLocation(key);
			}
			out = new ObjectOutputStream(bos);
			out.writeObject(value);
			
			Location location = bigFile.add(bos.toByteArray());
			addLocation(location);
		} catch (IOException e) {
			LOG.error("Error putting key:" + key, e);
		} finally {
			try {
				out.close();
				bos.close();
			} catch (Exception e) {
			}
		}
		return value;
	}

	private void deleteLocation(Object key) throws IOException {
		writer.deleteDocuments(new Term(KEY_FIELD, (String) key));
	}

	private void addLocation(Location location) throws IOException {
		Document doc = new Document();
        doc.add(new TextField("key", location.getKey(), Field.Store.YES));
        doc.add(new IntField("file", location.getFile(), Field.Store.YES));
        doc.add(new IntField("start", location.getStart(), Field.Store.YES));
        doc.add(new IntField("size", location.getSize(), Field.Store.YES));

        writer.addDocument(doc);
	}

	private Location findLocation(Object key) throws IOException {
		TermQuery query = new TermQuery(new Term(KEY_FIELD, (String) key));
		TopDocs docs = searcher.search(query, 1);
		Document document = reader.document(docs.scoreDocs[0].doc, fieldsToLoad);
		return convertToLocation(document);
	}
	
	private boolean locationExists(Object key) throws IOException {
		TermQuery query = new TermQuery(new Term(KEY_FIELD, (String) key));
		TopDocs docs = searcher.search(query, 1);
		return docs.totalHits > 0;
	}

	private Location convertToLocation(Document document) {
		String file = document.get("file");
		String start = document.get("start");
		String size = document.get("size");

		Location location = new Location(Integer.parseInt(file), Integer.parseInt(start),
				Integer.parseInt(size));
		location.setKey(document.get("key"));
		return location;
	}

	@Override
	public boolean containsKey(Object key) {
		try {
			return locationExists(key);
		} catch (IOException e) {
			LOG.error("Error finding key: " + key, e);
		}
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		throw new RuntimeException(UNIMPLEMENTED_METHOD);
	}

	@Override
	public Object get(Object key) {
		Object value = null;
		ObjectInput in = null;
		ByteArrayInputStream bis = null;
		try {
			Location location = findLocation(key);
				//objectLocations.get(key);
			if (location != null) {
				byte[] data = bigFile.get(location);

				bis = new ByteArrayInputStream(data);
				in = new ObjectInputStream(bis);
				value = in.readObject();
			}
		} catch (Exception e) {
			LOG.error("Error getting Key:" + key, e);
		} finally {
			try {
				in.close();
				bis.close();
			} catch (Exception e) {}
		}
		return value;
	}

	@Override
	public void clear() {
		bigFile.close();
	}

	@Override
	public int size() {
		return reader.numDocs();
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