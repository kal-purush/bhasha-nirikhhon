package ed_list;

class CNode{
	int value;
	CNode next;
	CNode prev;
	
	public CNode(int value){
		this.value = value;
	}
	public int getValue(){
		return value;
	}
	public void setValue(int value){
		this.value = value;
	}
	public CNode getNext(){
		return next;
	}
	public void setNext(CNode next){
		this.next = next;
	}
	public CNode getPrev(){
		return prev;
	}
	public void setPrev(CNode prev){
		this.prev = prev;
	}
	
}
public class CircularList implements List {
	CNode cursor;
	int size;
	
	public CircularList(){
		cursor = null;
		size = 0;
	}

	public void insert(int value, int pos) {
		CNode newNode = new CNode(value);
		if(size == 0){
			cursor = newNode;
			cursor.setNext(cursor);
			cursor.setPrev(cursor);
		}
		else if(pos == 0){
			newNode.setPrev(cursor.getPrev());
			cursor.getPrev().setNext(newNode);
			cursor.setPrev(newNode);
			newNode.setNext(cursor);
			cursor = newNode;
		}
		else if(pos == size){
			newNode.setNext(cursor);
			cursor.getPrev().setNext(newNode);
			newNode.setPrev(cursor.getPrev());
			cursor.setPrev(newNode);
		}
		else{
			CNode temp = cursor;
			for(int i = 0; i < pos - 1; i++){
				temp = temp.next;
			}
			newNode.setNext(temp.getNext());
			temp.getNext().setPrev(newNode);
			newNode.setPrev(temp);
			temp.setNext(newNode);
		}
		size++;
	}

	public void delete(int pos) {
		if(size == 1){
			cursor.setNext(null);
			cursor.setPrev(null);
		}
		else if(pos == 0){
			cursor.getPrev().setNext(cursor.getNext());
			cursor.getNext().setPrev(cursor.getPrev());
			cursor = cursor.next;
		}
		else if(pos == size - 1){   //ultima posicao
			cursor.getPrev().getPrev().setNext(cursor);
			cursor.setPrev(cursor.getPrev().getPrev());
			
		}
		else{
			CNode temp = cursor;
			for(int i = 0; i < pos - 1; i++){
				temp = temp.getNext();
			}
			temp.getNext().getNext().setPrev(temp);
			temp.setNext(temp.getNext().getNext());
		}
		size--;
	}
	
	public int find(int value) {
		CNode temp = cursor;
		for(int i = 0; i < size; i++){
			if(temp.value == value)
				return i;
			temp = temp.next;
		}
		throw new InvalidArgumentException();
	}

	public int elementAt(int pos) {
		if(pos == size - 1){  
			return cursor.getPrev().getValue();
		}
		else if(pos == 0){  //primeira posicao
			return cursor.getValue();
		}
		else{
			CNode temp = cursor;
			for(int i = 0; i < pos; i++)
				temp = temp.next;
			return temp.getValue();
		}
	}

	public void show() {
		for(int i = 0; i < size; i++){
			System.out.println(elementAt(i));
		}
	}

	public int size() {
		return size;
	}

	@Override
	public void troca(int i, int j) {
		// TODO Auto-generated method stub
		/*
		 * n�o foi utilizado nessa estrutura.
		 * */
	}
	    
	
}