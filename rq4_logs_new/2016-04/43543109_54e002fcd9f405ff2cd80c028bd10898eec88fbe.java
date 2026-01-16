package DList.src;

/**
 * Created by gjhawar on 10/28/15.
 */
public class DLink<E> {

    private E element;
    private DLink<E> next;
    private DLink<E> prev;

    public DLink(E data, DLink<E> nextvalue, DLink<E> prevValue){
        this.element = data;
        this.next = nextvalue;
        this.prev = prevValue;
    }

    DLink(DLink<E>nextvalue, DLink<E> prevValue){
        next = nextvalue;
        prev = prevValue;
    }
    //getter
    DLink<E> next(){
        return next;
    }

    DLink<E> prev(){
        return prev;
    }

    //setter
    DLink<E> setNext(DLink<E> nextvalue){
        return next = nextvalue;
    }
    DLink<E> setPrev(DLink<E> prevValue){
        return prev = prevValue;
    }

    E element(){
        return element;
    }

    E setElement(E data){
        return element = data;
    }

}