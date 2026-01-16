package com.badlogic.gdx.utils;

import java.io.IOException;
import java.io.Writer;

public abstract class DataWriter extends Writer{
	final Writer writer;
	
	DataWriter(Writer writer){
		this.writer = writer;
	}
	
	public void flush() throws IOException{
		writer.flush();
	}
	
	public void writeData(String name, Object text) throws IOException{
		first(name);
		packing(name, text);
	}
	
	public abstract DataWriter packing(String name, Object text) throws IOException;
	public abstract DataWriter first(String name) throws IOException;
	public abstract DataWriter pop() throws IOException;
	public abstract void write(char[] cbuf, int off, int len) throws IOException;
}