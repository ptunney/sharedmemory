package com.orbism.sharedmemory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.LinkedList;
import java.util.List;

public class BigFile {

	private int totalBytes;
	int currentFile;
	List<ByteBuffer>buffers;
	List<RandomAccessFile> backingFiles;

	private String fileLocation;
	private String filePrefix;

	public BigFile(int totalBytes, String fileLocation, String filePrefix) throws IOException {
		this.totalBytes = totalBytes;
		this.fileLocation = fileLocation;
		this.filePrefix = filePrefix;

		backingFiles = new LinkedList<RandomAccessFile>();
		RandomAccessFile file = new RandomAccessFile(new File(fileLocation + File.separator + filePrefix + "0.bin"), "rw");
		backingFiles.add(file);
		MappedByteBuffer buffer = file.getChannel().map(MapMode.READ_WRITE, 0, this.totalBytes * 8);

		currentFile = 0;
		buffers = new LinkedList<ByteBuffer>();
		buffers.add(buffer.asReadOnlyBuffer());
	}

	public Location add(byte[] data) throws IOException {
		int position = buffers.get(currentFile).position();
		if (position + data.length > totalBytes ) {
			currentFile++;
			RandomAccessFile file = new RandomAccessFile(new File( fileLocation + File.separator + filePrefix + currentFile +  ".bin"), "rw");
			backingFiles.add(file);

			MappedByteBuffer buffer = file.getChannel().map(MapMode.READ_WRITE, 0, totalBytes * 8);

			buffers.add(buffer.asReadOnlyBuffer());
			
			position = 0;
		}
		buffers.get(currentFile).put(data);
		return new Location(currentFile, position, data.length);
	}

	public byte[] get(Location location) {
		byte[] filedata = new byte[location.getSize()];
		ByteBuffer readBuffer = buffers.get(location.getFile()).duplicate();
		readBuffer.position(location.getStart());
		readBuffer.get(filedata, 0, location.getSize());
		return filedata;
	}

	public void update(Location location, byte[] data) {
		ByteBuffer readBuffer = buffers.get(currentFile).duplicate();
		readBuffer.position(location.getStart());
		readBuffer.put(data);
	}



	public void close() {
		for (RandomAccessFile file : this.backingFiles) {
			try {
				file.close();
			} catch (IOException e) {
				System.err.println("File " + file + " cannot be closed.");
			}
		}
		
		File temporaryFilesLocation = new File(this.fileLocation);
		File[] files = temporaryFilesLocation.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if (name.startsWith(filePrefix)) return true;
				
				return false;
			} 
		});
		
		for (File file : files) {
			file.delete();
		}
	}

	List<RandomAccessFile> getBackingFiles() {
		return this.backingFiles;
	}
}
