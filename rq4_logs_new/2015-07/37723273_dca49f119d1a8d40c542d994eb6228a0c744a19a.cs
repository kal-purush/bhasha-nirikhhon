using GUIdefs;
using HWdefs;

namespace miniFIFO
{
    public class myFIFO
    {
        private CNTRERROR[] atErrors = new CNTRERROR[(int)eListLenght.MAX_LIST_ERRORS];
        private uint nIn, nOut;
        private const uint MAXLIST_M1 = ((uint)eListLenght.MAX_LIST_ERRORS) - 1;
        public myFIFO() { nIn = nOut = 0; }
        public bool isEmpty() { return nIn == nOut ? true : false; }
        public bool isFilled() { return nIn == nOut ? false : true; }
        public bool isFree() { return ((nIn + 1) & MAXLIST_M1) == (nOut & MAXLIST_M1) ? false : true; }
        public void inErr(CNTRERROR tErrIn) { atErrors[nIn++ & MAXLIST_M1] = tErrIn; }
        public uint count() { return (nIn - nOut + (uint)eListLenght.MAX_LIST_ERRORS) & MAXLIST_M1; }
        public CNTRERROR outErr() { return atErrors[nOut++ & MAXLIST_M1]; }
        public void clear() { nIn = 0; nOut = 0; }
    }
}