package datastructure.list;

public class IntegerArrayList implements IntegerListInterface {

  private Integer[] item;
  private int numItems;
  private static final int DEFAULT_CAPACITY = 64;
  private final int NOT_FOUND = -12345;

  public IntegerArrayList() {
    item = new Integer[DEFAULT_CAPACITY];
    numItems = 0;
  }

  public IntegerArrayList(int n) {
    item = new Integer[n];
    numItems = 0;
  }

  @Override
  public void add(int index, Integer x) {
    if (numItems >= item.length || index < 0 || index > numItems) {
      // 에러 처리
    } else {
      for (int i = numItems - 1; i >= index; i--) // 1. 삽입 될 위치의 우측 원소들을 우측 시프트 
      {
        item[i + 1] = item[i]; // 우측 시프트 
      }
      item[index] = x; // 2. 원소 삽입
      numItems++; //
    }
  }

  @Override
  public void append(Integer x) {
    if (numItems >= item.length) {
      // 에러 처리 
    } else {
      item[numItems] = x;
    }
  }

  @Override
  public Integer remove(int index) {
    if (isEmpty() || index < 0 || index > numItems - 1) {
      return null;
    } else {
      Integer tmp = item[index];
      for (int i = index; i <= numItems - 2; i++) {
        item[i] = item[i + 1];
      }
      numItems--;
      return tmp;
    }
  }

  @Override
  public boolean removeItem(Integer x) {
    int k = 0;
    while (k < numItems && item[k].compareTo(x) != 0) {
      k++;
    }
    if (k == numItems) {
      return false;
    } else {
      for (int i = k; i <= numItems - 2; i++) {
        item[i] = item[i + 1];
      }
      numItems--;
      return true;
    }
  }

  @Override
  public Integer get(int index) {
    if (index >= 0 && index <= numItems - 1) {
      return item[index];
    } else {
      return null;
    }
  }

  @Override
  public void set(int index, Integer x) {
    if (index >= 0 && index <= numItems - 1) {
      item[index] = x;
    } else {
      //에러 처리
    }
  }

  @Override
  public int indexOf(Integer x) {
    int i = 0;
    for (i = 0; i < numItems; i++) {
      if (((Comparable) item[i]).compareTo(x) == 0) {
        return i;
      }
    }
    return NOT_FOUND;
  }

  @Override
  public int len() {
    return numItems;
  }

  @Override
  public boolean isEmpty() {
    return numItems == 0;
  }

  @Override
  public void clear() {
    item = new Integer[item.length];
    numItems = 0;
  }
}