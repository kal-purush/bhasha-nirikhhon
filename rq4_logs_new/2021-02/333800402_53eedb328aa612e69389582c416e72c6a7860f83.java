package JAVA_LEVEL_2.Java_lvl2_homework.git.lesson2.hw2;

public class CreateMassive {
    private final int lengthMassive;
    private final int heightMassive;
    public  CreateMassive() {
        this.heightMassive = 4;
        this.lengthMassive = 4;
    }

    public String[][] createMass(int length,int height) throws MyArraySizeException, MyArrayDataException {
        String array[][] = new String[length][height];
        if(length!=lengthMassive||height!=heightMassive){
            throw new MyArraySizeException();
        }
        for (int i = 0; i < length; i++) {
            for (int j = 0; j < height; j++) {
                array[i][j] = generateCeil();
//                if (Integer.parseInt(array[i][j])!=0||Integer.parseInt(array[i][j])!=1||Integer.parseInt(array[i][j])!=2||Integer.parseInt(array[i][j])!=3||Integer.parseInt(array[i][j])!=4
//                        ||Integer.parseInt(array[i][j])!=5||Integer.parseInt(array[i][j])!=6||Integer.parseInt(array[i][j])!=7||Integer.parseInt(array[i][j])!=80||Integer.parseInt(array[i][j])!=9){
//                    //throw new MyArrayDataException("Ошибка в ячейке:" + i + "-" + j);
//                }
            }
        }
        return array;
    }

    public String generateCeil(){
        //String[] alphabet = {"a","b","c","d","e","e","f","g","h","i","g","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"," ",
                            //"1","2","3","4","5","6","7","8","9","0","/","!","$","#","%","^","&","*","(",")","{","}"};
        String[] alphabet ={"a","2","3","4","5","6","7","8","9","0"};
        String newString = "";
        for (int i = 0; i < 1; i++) {
            newString = newString + alphabet[(int)(Math.random()* alphabet.length)];
        }

        return newString;
    }


}
    class MyArraySizeException extends Exception{

    }
    class MyArrayDataException extends Exception{
        public MyArrayDataException(String message){
            super(message);
        }
    }