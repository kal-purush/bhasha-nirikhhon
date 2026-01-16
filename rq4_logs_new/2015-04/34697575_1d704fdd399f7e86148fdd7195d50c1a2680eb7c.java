
public class SparseMatWMult extends SparseMat<Double>
{

   public SparseMatWMult(int rowSize, int colSize, Double defaultVal)
   {
      super(rowSize, colSize, defaultVal);

   }

   public SparseMat<Double> transposeMat(SparseMatWMult primeMat) throws CloneNotSupportedException
      {
       SparseMatWMult matTemp
           = new SparseMatWMult(primeMat.rowSize, primeMat.colSize, 0.);
      for (int r = 0; r < primeMat.rowSize; r++)
      {
         for (int c = 0; c < primeMat.colSize; c++)
         { 
           Double temp = primeMat.get(r, c).doubleValue();
  
            matTemp.set(c, r, temp);
         }
      }
      primeMat = (SparseMatWMult) matTemp.clone();
      
      return primeMat;
   }

}