enum Gradelevel { 
        
        twelve("12"),
        eleven("11"),
        ten("10"),
        nine("9"),
        eight("8"),
        seven("7"),
        six("6"),
        five("5"),
        four("4"),
        three("3"),
        two("2"),
        one("1"),
        sk("Senior Kindergarten"),
        jk("Junior Kindergarten");
                
  private final String stringGrade;
                
  Gradelevel(String grade) {
    this.stringGrade = grade;
  }
         
  public String setGrade() {
    return this.stringGrade;
  }
}