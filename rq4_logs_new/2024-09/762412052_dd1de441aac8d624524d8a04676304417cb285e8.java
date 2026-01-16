package sptech.school.projetovolt.utils;

import org.springframework.stereotype.Component;
@Component
public class HashTableObj<T> {
    private ListaEncadeadaObj<T>[] tab;

    public HashTableObj() {
        this.tab = new ListaEncadeadaObj[26];
        for (int i = 0; i < tab.length; i++) {
            tab[i] = new ListaEncadeadaObj<T>();
        }
    }
    private int hashFunction(T product){
        return Math.abs(product.hashCode())%tab.length;
    }
    public void put(T product){
        int key = hashFunction(product);
        tab[key].addNode(product);
    }
    public T get(T product){
        int key = hashFunction(product);
        NodeObj<T> nodeFinded = tab[key].searchNode(product);
        if(nodeFinded == null){
            return null;
        }
        return nodeFinded.getInfo();
    }
    public void show(){
        for (int i = 0; i < tab.length; i++) {
            System.out.print("\nEntrada " + i  +" :");
            if(tab[i].getSize() == 0){
                System.out.print("Lista vazia");
            }else{
                tab[i].show();
            }
        }
    }
    public Boolean isEmpty(){
        int aux = 0;
        for (int i = 0; i < tab.length; i++) {
            if(tab[i].getSize() == 0){
                aux++;
            }
        }
        if(aux == tab.length){
            return true;
        }
        return false;
    }
    public Boolean remove(T product){
        int key = hashFunction(product);
        if(tab[key].removeNode(product)){
            return true;
        }
        return false;
    }
    public int[] size(){
        int[] bucketSizes = new int[26];
        for (int i = 0; i < tab.length; i++) {
            bucketSizes[i] = tab[i].getSize();
        }
        return bucketSizes;
    }
}


