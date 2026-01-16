package dat.dev;

public class JavaOOP {
    public static void main(String [] agrs) {
    	final String arrName[]= {"Quoc Dat","Khac Cuong","Phi Hung","Bao Co","Hong Quan"};
    	final double arrdiemtoan[]= {6,7,8,9,10};
    	final double arrdiemly[]= {9,9,9,8,8};
    	final double arrdiemhoa[]= {7,8,9,9,7};
    	SinhVien arrSinhVien[]= new SinhVien[arrName.length];
    	for(int i=0;i<arrSinhVien.length;i++) {
    		SinhVien sv= new SinhVien(arrName[i],arrdiemtoan[i],arrdiemly[i],arrdiemhoa[i]);
    		arrSinhVien[i]=sv;
    	}
    	for(int i=0;i<arrSinhVien.length;i++) {
    		if(arrSinhVien[i].diemTrungBinh(arrdiemtoan[i],arrdiemly[i],arrdiemhoa[i])>8) {
    			arrSinhVien[i].show();
    		}
    	}
    }
}