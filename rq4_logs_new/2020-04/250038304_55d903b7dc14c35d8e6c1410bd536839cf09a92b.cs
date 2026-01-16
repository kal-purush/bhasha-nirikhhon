using System;

namespace ODWai2.ODWaiCore.Controllers
{
    public class ODWaiDetector
    {
        public static void start_detection(string graph_path,
                                           string root_x,
                                           string root_y,
                                           string width,
                                           string height,
                                           Action completion = null)
        {
            System.Threading.Thread.Sleep(2000);
            completion?.Invoke();
        }
    }
}