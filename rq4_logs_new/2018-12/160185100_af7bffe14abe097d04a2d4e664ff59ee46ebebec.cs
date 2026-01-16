using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Drawing;
using System.Windows.Forms;

namespace LBMace
{
    /**
    * @brief 시뮬레이션에 사용할 변수를 초기화 하는 클래스 \n
    * 본 시뮬레이션에서 사용되는 변수는 전부 data 클래스에 저장되며, data 클래스는 Singleton 패턴을 이용하여 관리의 용이성을 향상하였다.
    */
    class Data
    {
        /** @brief data 클래스를 싱글톤으로 설정하는 코드 */
        private static Data data;

        /** @brief Computation domain인 lattice의 크기 */
        public int[] size;
        /** @brief Inlet에 해당하는 lattice의 번호를 저장함 */
        public int[] inletID;
        /** @brief 각 lattice의 type(solid, fluid, wall)을 저장함 */
        public int[] map;
        /** @brief inlet velocity 설정
        * Reynolds # 계산에 사용 됨
        */
        public double u0;
        /** @brief Exchange rate를 의미함*/
        public double xrate;
        /** @brief 유동의 Reynolds #를 저장함 */
        public double Re;

        /** @brief distribution function의 초기값 */
        public double[] fin;
        /** @brief distribution function의 collision step 이후의 값 */
        public double[] fout;
        /** @brief distribution function의 zeroth moment인 density\n
        * density는 Macroscopic variable이다.
        */
        public double[] density;
        /** @brief distribution function의 first moment인 velocity의 x vector\n 
        * ux는 Macroscopic variable이다.
        */
        public double[] ux;
        /** @brief distribution function의 first moment인 velocity의 y vector\n 
        * uy는 Macroscopic variable이다.
        */
        public double[] uy;
        /** @brief steady-state 검사용 배열\n
        * 전체 simulation domain에 대해 iteration 간의 ux 값 차를 구하여 steady-state를 검사함
        */
        public double[] criteria;
        /** @brief criteria 계산을 위한 버퍼\n
        * 이전 iteration의 velocity를 저장함.
        */
        public double[] up, vp;
        /** @brief strain rate tensor의 값을 저장함.
        */
        public double[] strain;
        /** @brief dynamic pressure를 저장 함.
        */
        public double[] dynamic;
        /** @brief 메시지 저장을 위한 StringBulider 객체
        */
        public StringBuilder sb;
        /** @brief inlet lattice의 길이\n
        * Reynolds # 계산에 사용 됨
        */
        private double inletLeng;

        /** @brief relaxation time을 계산함
        */
        public double tau
        {
            get
            {
                return u0 * inletLeng / (this.Re) * 3.0d + 0.5d;
            }
        }


        private Data()
        {
            /** 2D에 대한 변수 설정 */
            inletID = new int[4];
            size = new int[2];
            sb = new StringBuilder();
        }

        /** @brief data의 싱글턴 생성을 위한 함수\n
        * get을 호출 할 때, data 클래스가 생성되어 있지 않다면 생성 후 리턴함\n
        * @return data 클래스 그 자체
        */
        public static Data get()
        {
            if(data == null)
            {
                data = new Data();
            }
            return data;
        }

        /** @brief inletLeng과 Re, u0를 설정하는 함수
        * @param Re Reynolds #
        * @param u0 initial velocity
        */
        public void relaxationTime(double Re, double u0)
        {
            double x0, x1, y0, y1;
            x0 = inletID[0] % size[0];
            x1 = inletID[1] % size[0];
            y0 = inletID[0] / size[0];
            y1 = inletID[1] / size[0];

            inletLeng = Math.Abs(Math.Sqrt(Math.Pow((x1 - x0), 2) + Math.Pow((y1 - y0), 2)));
            this.Re = Re;

            // viscosity = 0.04d;
            // tau = 3d * viscosity + 0.5d;
            
            // u0 = ((tau-0.5d)/3d) * this.Re / inletLeng;
            this.u0 = u0;
        }

        /** @brief map 데이터를 읽어와서 simulation domain을 초기화
        * @return 메소드 수행 성공 여부
        */
        public bool mapping()
        {
            Bitmap tiles_ = new Bitmap(10,10);

            if (loadFile(ref tiles_))
            {
                size = sizing(tiles_);
                map = getColor(tiles_);
                inletID = counterColor();

                init();
                return true;
            }
            return false;
        }

        /** @brief 각종 시뮬레이션 관련 변수 선언 및 초기화
        */
        private void init()
        {
            int area = size[0] * size[1];

            /* D2Q9 scheme의 weighting factor */
            double[] weight = new double[9] { 4d / 9d, 1d / 9d, 1d / 9d, 1d / 9d, 1d / 9d, 1d / 36d, 1d / 36d, 1d / 36d, 1d / 36d };

            fin = new double[area * 9];
            fout = new double[area * 9];
            density = new double[area];
            ux = new double[area];
            uy = new double[area];
            up = new double[area];
            vp = new double[area];
            criteria = new double[area];
            strain = new double[area];
            dynamic = new double[area];

            for (int index = 0; index < area; index++)
            {
                density[index] = 1d;
                ux[index] = 0;
                uy[index] = 0;
                up[index] = 0;

                for (int n = 0; n < 9; n++)
                {
                    fin[n + 9 * index] = weight[n];
                    fout[n + 9 * index] = weight[n];
                }
            }
        }

        /** @brief Geometry를 Bitmap으로부터 로딩하는 method
        * @return 성공 여부
        */
        private bool loadFile(ref Bitmap tiles)
        {
            OpenFileDialog ofd = new OpenFileDialog();
            ofd.DefaultExt = ".bmp";
            ofd.Filter = "Map file (.bmp)|*.bmp";

            ofd.ShowDialog();

            if(ofd.FileName != "")
            {
                tiles = new Bitmap(Image.FromFile(ofd.FileName));
                return true;
            }
            
            return false;
        }

        /** @brief 로딩한 geometry의 크기를 simulation domain의 크기로 설정함
        * @return 2D simulation domain의 크기를 가진 int 배열
        */
        private int[] sizing(Bitmap tiles)
        {
            int[] lengs = new int[2];

            lengs[0] = tiles.Width;
            lengs[1] = tiles.Height;

            return lengs;
        }

        /** @brief 로딩한 Bitmap의 색상정보로부터 각 lattice의 type을 구분함\n
        * White(0): Fluid, \n
        * Black(1): Wall, \n
        * Red(2): Inlet(left), \n
        * Blue(3): Inlet(Right), \n
        * Green(4): Outlet\n
        * @return 2D simulation domain의 lattice 정보
        */
        private int[] getColor(Bitmap tiles)
        {
            List<int> result = new List<int>();

            var output =
                from j in Enumerable.Range(0, tiles.Height)
                from i in Enumerable.Range(0, tiles.Width)
                select tiles.GetPixel(i, j);
            
            foreach(var tile in output)
            {
                if (tile == Color.FromArgb(255, 255, 255, 255))
                {
                    result.Add(0);
                }
                else if (tile == Color.FromArgb(255, 0, 0, 0))
                {
                    result.Add(1);
                }
                else if (tile == Color.FromArgb(255, 255, 0, 0))
                {
                    result.Add(2);
                }
                else if (tile == Color.FromArgb(255,0,0,255))
                {
                    result.Add(3);
                }
                else
                {
                    result.Add(4);
                }
            }
            
            return result.ToArray();
        }

        /** @brief inletID 계산을 위한 색상정보 검색
        * @return inletID에 저장될 값
        */
        private int[] counterColor()
        {
            int[] output = new int[4];
            int cntRed, cntBlue;

            cntRed = 0;
            cntBlue = 0;

            for (int index = 0; index < size[0] * size[1]; index++)
            {
                if (map[index] == 2)
                {
                    if(cntRed == 0)
                    {
                        output[0] = index;
                    }
                    output[1] = index;
                    cntRed++;
                }
                if (map[index] == 3)
                {
                    if(cntBlue == 0)
                    {
                        output[2] = index;
                    }
                    output[3] = index;
                    cntBlue++;
                }
            }

            return output;
        }
    }
}