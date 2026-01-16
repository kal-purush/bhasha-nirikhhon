namespace ChatApplication.Comparers
{
    class FileTransferContainerCompareFileName
    {
        public static int CompareAscending(DataContainers.FileTransferContainer a, DataContainers.FileTransferContainer b)
        {
            if (a == null) {
                if (b == null) {
                    return 0;
                }
                else {
                    return -1;
                }
            }
            else {
                if (b == null) {
                    return 1;
                }
                int _resultOfComparison = string.CompareOrdinal(a.fileName, b.fileName);
                if(_resultOfComparison < 0) {
                    return -1;
                }else if(_resultOfComparison > 0) {
                    return 1;
                }
                else {
                    return 0;
                }
            }
        }

        public static int CompareDescending(DataContainers.FileTransferContainer a, DataContainers.FileTransferContainer b)
        {
            int result = CompareAscending(a, b);
            if (result == 1) {
                return -1;
            }
            else if (result == -1) {
                return 1;
            }
            else {
                return 0;
            }
        }
    }
}