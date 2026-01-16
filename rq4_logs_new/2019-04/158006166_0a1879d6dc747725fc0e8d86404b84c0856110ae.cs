namespace LeedCodeBusiness.Algos.easy
{
    //Number 7
    public class ReverseInteger
    {
        public int ReverseOneInteger(int IntegerValue)
        {
            bool valueType = false;
            if (IntegerValue < 0)
            {
                valueType = true;
            }     

            string text = IntegerValue.ToString();

            char[] cArray = text.ToCharArray();
            string reverse = System.String.Empty;
            
            for (int i = cArray.Length - 1; i > -1; i--)
            {
              
                reverse += cArray[i];
                if (valueType == true && 0 == i)
                {
                    reverse = reverse.Remove(cArray.Length - 1);
                    reverse = reverse.Insert(0, "-");
                }
            }

            try
            {
                var rev = int.Parse(reverse);

                return rev;
            }
            catch (System.Exception)
            {
                return 0;
                
            }        
            
        }
    }


}