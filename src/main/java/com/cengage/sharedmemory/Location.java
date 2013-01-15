package com.cengage.sharedmemory;

public class Location {
	private int file;
	private int start;
	private int size;
	private String key;

	public Location(int currentFile, int position, int length) {
		this.file = currentFile;
		this.start = position;
		this.size = length;
	}

	public int getFile() {
		return file;
	}
	public void setFile(int file) {
		this.file = file;
	}
	public int getStart() {
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
}