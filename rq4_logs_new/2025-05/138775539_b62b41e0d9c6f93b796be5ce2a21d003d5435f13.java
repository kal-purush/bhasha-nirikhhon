package my.com.app;

import java.util.*;

class MyCircularQueue {

    int[] data;
    int head;
    int tail;
    int size;
    
    public MyCircularQueue(int k) {
        data = new int[k];
        tail = -1;
        head = -1;
        size = 0;
    }
    
    public boolean enQueue(int value) {
        // if queue is empty or full or has space 
        
        
        if (isFull()){
            return false;
        }
        
        if(isEmpty()){
            data[++tail] = value;
            head = tail;
        } else {
        // two cases tail is less than data.lenght
        // tail is equal to data.length but , queue is not full, set tail to 0 and insert element
        
            if ( tail == data.length-1){
                tail = 0 ;
                data[tail] = value;
            } else {
                data[++tail] = value;
            }
        }
        size++;
        
        return true;
    }
    
    public boolean deQueue() {
        
        if(isEmpty()){
            return false;
        }
        
        if (head == tail) {
            head = -1;
            tail = -1;
            size--;
            return true;
        }

        if ( head == data.length-1){
            head = 0 ;
        } else {
            head++;
        }
        size--;


         return true;
    }
    
    public int Front() {
        if ( !isEmpty()){
            return data[head];
        }
        
        return -1;
    }
    
    public int Rear() {
        if ( !isEmpty()){
            return data[tail];
        }
        
        return -1;
    }
    
    public boolean isEmpty() {
        return head == -1;
    }
    
    public boolean isFull() {
        
        return size == data.length;
    }
}

/**
 * Your MyCircularQueue object will be instantiated and called as such:
 * MyCircularQueue obj = new MyCircularQueue(k);
 * boolean param_1 = obj.enQueue(value);
 * boolean param_2 = obj.deQueue();
 * int param_3 = obj.Front();
 * int param_4 = obj.Rear();
 * boolean param_5 = obj.isEmpty();
 * boolean param_6 = obj.isFull();
 */