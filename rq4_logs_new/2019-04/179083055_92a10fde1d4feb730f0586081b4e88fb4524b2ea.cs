using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SapXep
{
   public class ThuatToan
    {
        public static List<DanhSach> BubbleSort_Ten(List<DanhSach> Input)
        {

            for (int i = 0; i < Input.Count; i++)
            {
                for (int j = Input.Count - 1; j > i; j--)
                {
                    if (SoSanhDanhSach(Input[j], Input[j - 1], SoSanh.TEN) == Input[j].ID)
                    {
                        DanhSach tp = Input[j];
                        Input[j] = Input[j - 1];
                        Input[j - 1] = tp;
                    }
                }
            }
            return Input;
        }
        public static List<DanhSach> SelectionSort_Ten(List<DanhSach> Input)
        {
            int min;
            for (int i = 0; i < Input.Count; i++)
            {
                min = i;
                for (int j = i + 1; j < Input.Count; j++)
                {
                    if (SoSanhDanhSach(Input[j], Input[min], SoSanh.TEN) == Input[j].ID)
                    {
                        min = j;
                    }
                }
                DanhSach temp = Input[i];
                Input[i] = Input[min];
                Input[min] = temp;
            }
            return Input;
        }

        static List<DanhSach> TenNam(List<DanhSach> Input)
        {
            List<DanhSach> Output = new List<DanhSach>();
            for (int a = 0; a < Input.Count; a++)
            {
                if (Input[a].GioiTinh == "Nam")
                {
                    Output.Add(Input[a]);
                }
            }
            return Output;
        }
        static List<DanhSach> TenNu(List<DanhSach> Input)
        {
            List<DanhSach> Output = new List<DanhSach>();
            for (int a = 0; a < Input.Count; a++)
            {
                if (Input[a].GioiTinh == "Nu")
                {
                    Output.Add(Input[a]);
                }
            }
            return Output;
        }
        public static List<DanhSach> SapXepTenTheoGT(List<DanhSach> Input)
        {
            List<DanhSach> Output = SoSanhTheoTen(TenNam(Input));
            foreach (DanhSach Tnu in SoSanhTheoTen(TenNu(Input)))
            {
                Output.Add(Tnu);
            }
            return Output;
        }

        public static List<DanhSach> SoSanhTheoTuoi(List<DanhSach> Input)
        {
            List<DanhSach> Output = new List<DanhSach>();
            for (int i = 0; i < Input.Count; i++)
            {
                int x = -1;

                for (int j = 0; j < Output.Count; j++)
                {
                    if (SoSanhDanhSach(Input[i], Output[j], SoSanh.TUOI) == Input[i].ID)
                    {
                        x = j;
                        break;
                    }
                }
                if (x >= 0)
                {
                    Output.Insert(x, Input[i]);
                }
                else
                {
                    Output.Add(Input[i]);
                }
            }
            return Output;
        }
        public static List<DanhSach> SoSanhTheoGioiTinh(List<DanhSach> Input)
        {
            List<DanhSach> Output = new List<DanhSach>();
            for (int i = 0; i < Input.Count; i++)
            {
                int x = -1;

                for (int j = 0; j < Output.Count; j++)
                {
                    if (SoSanhDanhSach(Input[i], Output[j], SoSanh.GIOITINH) == Input[i].ID)
                    {
                        x = j;
                        break;
                    }
                }
                if (x >= 0)
                {
                    Output.Insert(x, Input[i]);
                }
                else
                {
                    Output.Add(Input[i]);
                }
            }
            return Output;
        }
        public static List<DanhSach> SoSanhTheoTen(List<DanhSach> Input)
        {
            List<DanhSach> Output = new List<DanhSach>();
            for (int i = 0; i < Input.Count; i++)
            {
                int x = -1;

                for (int j = 0; j < Output.Count; j++)
                {
                    if (SoSanhDanhSach(Input[i], Output[j], SoSanh.TEN) == Input[i].ID)
                    {
                        x = j;
                        break;
                    }
                }
                if (x >= 0)
                {
                    Output.Insert(x, Input[i]);
                }
                else
                {
                    Output.Add(Input[i]);
                }
            }
            return Output;
        }
        public static List<DanhSach> SoSanhTheoHoTen(List<DanhSach> Input)
        {
            List<DanhSach> Output = new List<DanhSach>();
            for (int i = 0; i < Input.Count; i++)
            {
                int x = -1;

                for (int j = 0; j < Output.Count; j++)
                {
                    if (SoSanhDanhSach(Input[i], Output[j], SoSanh.HOTEN) == Input[i].ID)
                    {
                        x = j;
                        break;
                    }
                }
                if (x >= 0)
                {
                    Output.Insert(x, Input[i]);
                }
                else
                {
                    Output.Add(Input[i]);
                }
            }
            return Output;
        }

        static int SoSanhDanhSach(DanhSach a, DanhSach b, SoSanh ThuocTinh)
        {
            if (ThuocTinh == SoSanh.TUOI)
            {
                if (a.Tuoi < b.Tuoi)
                {
                    return a.ID;
                }
            }
            else if (ThuocTinh == SoSanh.HOTEN)
            {
                if (SoSanhString(a.HoTen, b.HoTen) == a.HoTen)
                {
                    return a.ID;
                }
            }
            else if (ThuocTinh == SoSanh.TEN)
            {
                if (SoSanhString(a.Ten, b.Ten) == a.Ten)
                {
                    return a.ID;
                }
            }
            else if (ThuocTinh == SoSanh.GIOITINH)
            {
                if (SoSanhString(a.GioiTinh, b.GioiTinh) == a.GioiTinh)
                {
                    return a.ID;
                }
            }
            return b.ID;
        }
        static String SoSanhString(string _a, string _b)
        {
            string a = _a ?? "";
            string b = _b ?? "";
            for (int i = 0; i < Math.Min(a.Length, b.Length); i++)
            {
                if (a[i] < b[i])
                {
                    return a;
                }
                else if (a[i] > b[i])
                {
                    return b;
                }
                else if (a[i] == b[i])
                {
                    return b;
                }
            }
            return b;
        }
        
    }
}