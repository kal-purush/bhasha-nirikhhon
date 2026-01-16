public class KiemChungVien extends NhanVien {
    int soLoiPhatHien;

    public KiemChungVien(int maNV, String hoTen, int tuoi, String soDienThoai, String email, float luongCoBan,
            int soLoiPhatHien) {
        super(maNV, hoTen, tuoi, soDienThoai, email, luongCoBan);
        this.soLoiPhatHien = soLoiPhatHien;
    }

    @Override
    public float tinhLuong() {
        super.tinhLuong();
        return (luongCoBan + soLoiPhatHien * 50000);
    }
}