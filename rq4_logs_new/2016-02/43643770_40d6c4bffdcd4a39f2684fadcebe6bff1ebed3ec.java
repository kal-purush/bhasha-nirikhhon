package ch19.ex19_01;

import java.util.Objects;

/**
 * リストの一つを示すクラス。リストの要素と次の{@Link LinkedList}への参照を保持する。次の{@Link LinkedList}への参照がnullのときリストの終端。
 */
public class LinkedList {

	private Object element;
	private LinkedList next;

	/**
	 * 与えられた要素と次のリストへの参照を用いてリストを作成する。
	 * @see LinkedList(Object)
	 */
	public LinkedList(Object element, LinkedList next) {
		this.element = element;
		this.next = next;
	}

	/**
	 * 与えられた要素を用いてリストを作成する。次のリストへの参照には、リストの終端を示すnullを設定する。
	 * @see LinkedList(Object, LinkedList)
	 */
	public LinkedList(Object element) {
		this(element, null);
	}

	/** このリスト内に包含している要素を返す。 */
	public Object getElement() {
		return element;
	}

	/** このリストに引数で与えるオブジェクトを要素として包含する */
	public void setElement(Object element) {
		this.element = element;
	}

	/** このリストの次のリストへの参照を返す */
	public LinkedList getNext() {
		return next;
	}

	/** このリストの次のリストへの参照を設定する */
	public void setNext(LinkedList next) {
		this.next = next;
	}

	@Override
	public String toString() {
		StringBuilder listContents = null;
		if (element != null) {
			listContents = new StringBuilder("[element: " + element.toString() + "]");
		} else {
			listContents = new StringBuilder("[element: null]");
		}
		if (next != null) {
			listContents.append(next.toString());
		}
		return listContents.toString();
	}
	
	/**
	 * リストの要素数を返す。
	 * @param head リスト構造の先頭を指す参照
	 * @return headの指すリスト構造の要素数
	 **/
	public static int size(LinkedList head) {
		Objects.requireNonNull(head, "head is null");
		int size = 1;
		LinkedList next = head.getNext();
		while (next != null) {
			size++;
			next = next.getNext();
		}
		return size;
	}
}