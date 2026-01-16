public interface IList {

	public void addFirst(String Origen,String Destino,String idUsuario);
	
	public void addLast(String Origen,String Destino,String idUsuario);

	public void removeFirst();

	public void removeLast();
	
	public void insertAt(int index, String Origen,String Destino,String idUsuario);
	
	public boolean isEmpty();

	public boolean contains(String Origen,String Destino,String idUsuario);

	public int getSize();

	public int getIndexOf(String Origen,String Destino,String idUsuario);

	public Request getFirst();

	public Request getLast();

	public Request getAt(int index);

	public void removeAll(String Origen,String Destino,String idUsuario);

	public void removeAt(int index);

}

