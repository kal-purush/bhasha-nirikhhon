class Exchange{
  int id;
  Exchange parent;
  Boolean isRoot;
  ExchangeList children;
  MobilePhoneSet residentSet;

  public Exchange(int i){
    this.id = i;
    if(i == 0) {
      isRoot = true;
      parent = null;
    }
    else isRoot = false;
    children = new ExchangeList();
    residentSet = new MobilePhoneSet();
  }

  public Exchange parent(){
    return parent;
  }

  public int numChildren(){
    return children.size();
  }

  public Exchange child(int i){
    Node child = children.l.head;
    try{
      if(i < children.size() && i >= 0){
        for(int j=0;j<i;j++){
          child = child.next;
        }
        Exchange m = (Exchange) child.data;
        return m;
      } else {
        System.out.println("index out of range");
        return null;
      }
    } catch(Exception e){
      e.printStackTrace();
      return null;
    }
  }

  public Boolean isRoot(){
    if(isRoot) return true;
    else return false;
  }

  public RoutingMapTree subtree(int i){
    RoutingMapTree tree = new RoutingMapTree();
    Exchange ex = child(i);
    ex.isRoot = true;
    ex.parent = null;
    tree.root = ex;
    if(tree != null) return tree;
		return null;
  }

  public MobilePhoneSet residentSet(){
    return residentSet;
  }
}

class ExchangeList{
  LinkedList l;

  public ExchangeList(){
    l = new LinkedList();
    l.head = null;
  }

  public int size(){
    return l.length();
  }
}