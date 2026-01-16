package Session4.assigment4;

import java.util.ArrayList;

public class DanhSachLopHoc {
    public String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSoluonghocsinh() {
        return soluonghocsinh;
    }

    public void setSoluonghocsinh(int soluonghocsinh) {
        this.soluonghocsinh = soluonghocsinh;
    }

    public ArrayList getClassList() {
        return ClassList;
    }

    public void setClassList(ArrayList classList) {
        ClassList = classList;
    }

    public int soluonghocsinh;
    ArrayList ClassList = new ArrayList();

    public DanhSachLopHoc(String name, int soluonghocsinh) {
        this.name = name;
        this.soluonghocsinh = soluonghocsinh;
    }
}