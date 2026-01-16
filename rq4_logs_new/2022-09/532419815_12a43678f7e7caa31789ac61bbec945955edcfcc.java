package Antivirus;

public class Cilent {
    public static void main(String[] args) throws Exception {
        Folder Folder1,Folder2,Folder3;
        Folder1 = new Folder("文件夹1");
        Folder2 = new Folder("文件夹222");
        Folder3 = new Folder("文件夹3333");

        lmageFile lmage1,lmage2,lmage3;
        lmage1 = new lmageFile("图像1");
        lmage2 = new lmageFile("图像2");
        lmage3 = new lmageFile("图像3");

        TextFile Text1,Text2;
        Text1 = new TextFile("文本1");
        Text2 = new TextFile("文本2");

        VideoFile Video1 = new VideoFile("视频1");

        Folder1.add(Folder2);
        Folder1.add(Folder3);
        Folder1.add(lmage3);

        Folder2.add(lmage1);
        Folder2.add(lmage2);
        Folder2.add(Text1);

        Folder3.add(Video1);
        Folder3.add(Text2);

        Folder1.killVlrus();
    }
}