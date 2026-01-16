using System;

namespace THBaoMat
{
    class Euclid
    {
        // Hàm Euclid mở rộng để tính số nghịch đảo
        public int gcd(int b, int a)
        {

            int r1 = a, r2 = b;

            int t1 = 0, t2 = 1;
            int q, r, t;

            // Áp dụng thuật toán Euclid mở rộng
            while (r2 > 0)
            {
                q = r1 / r2;  // Tính thương
                r = r1 - q * r2;  // Tính phần dư
                r1 = r2;
                r2 = r;


                t = t1 - q * t2;
                t1 = t2;
                t2 = t;
            }

            // Nếu gcd là 1, t1 chính là số nghịch đảo của a mod b
            if (r1 == 1)
            {
                // Đảm bảo t1 luôn là số dương trong modulo b
                if (t1 < 0)
                {
                    t1 += a;  // Điều chỉnh t1 nếu nó âm
                }
                return t1;
            }

            // Nếu gcd khác 1, không có nghịch đảo
            return -1;  // Trả về -1 nếu không có số nghịch đảo
        }


    }
}