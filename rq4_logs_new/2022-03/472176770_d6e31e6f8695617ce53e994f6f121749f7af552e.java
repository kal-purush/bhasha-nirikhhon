package Session4.assigment4;

public class Main {
    public static void main(String[] args) {
        MonHocDaiCuong mhdc = new MonHocDaiCuong();
        mhdc.themLopHoc("lop1", 12);
        mhdc.themLopHoc("Lop2", 13);
        mhdc.themLopHoc("Lop3", 15);
        mhdc.themLopHoc("Lop4", 10);
//        for (DanhSachLopHoc ds : mhdc.ClasList){
//            System.out.println(ds.name);
//            System.out.println(ds.soluonghocsinh);
//        }
//        mhdc.xoaLopHoc("lop1");
//        for (DanhSachLopHoc ds : mhdc.ClasList){
//            System.out.println(ds.name);
//            System.out.println(ds.soluonghocsinh);
//        }

        mhdc.sapXep();
        mhdc.inDanhSach();

    }
}