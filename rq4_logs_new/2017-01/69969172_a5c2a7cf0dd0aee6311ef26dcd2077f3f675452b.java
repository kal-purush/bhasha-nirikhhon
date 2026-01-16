package de.thorbenkuck.rhfw.pipe.duplicate;

import java.io.*;

public class DeepCloneDuplicator {

	public Object cloneSerializable(Object o) {
		ObjectOutputStream oos = null;
		ObjectInputStream ois = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(bos);
			oos.writeObject(o);
			oos.flush();
			ByteArrayInputStream bin = new ByteArrayInputStream(bos.toByteArray());
			ois = new ObjectInputStream(bin);
			return ois.readObject();
		}
		catch(Exception e) {
			return null;
		}
		finally {
			try {
				if(oos != null) {
					oos.close();
				}
				if(ois != null) {
					ois.close();
				}
			} catch (IOException ignored) {}
		}
	}

}