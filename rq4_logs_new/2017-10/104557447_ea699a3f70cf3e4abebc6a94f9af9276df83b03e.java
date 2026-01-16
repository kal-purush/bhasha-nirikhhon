package e03;

import java.io.*;
import java.util.*;

public class MyUtil {

    public MyUtil() {
        // TODO Auto-generated constructor stub
    }

    /**
     * ��������û���Ͻ���Ƭ��ѧ��ѧ��
     *
     * @param studentListFilename ѧ�������ļ�
     * @param dir
     * @return
     */
    public Set<String> getNoPicStudentIDS(String studentListFilename, String dir) throws IOException {
        InputStream is = new FileInputStream(studentListFilename);
        String line;
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "gbk"));
        line = reader.readLine();
        List<String> ids = new ArrayList<>();
        while (line != null) {
            for (int i = 0; i < line.length(); i++) {
                if (!(line.charAt(i) >= '0' && line.charAt(i) <= '9')) {
                    ids.add(line.substring(0, i));
                    break;
                }
            }
            line = reader.readLine();
        }
        File[] files = new File(dir).listFiles();
        List<String> picIDs = new ArrayList<>();
        if (files != null) {
            for (File file : files) {
                picIDs.add(getStudentID(file.getName()));
            }
        }
        List<String> gonnaDelete = new ArrayList<>();
        for (String s : ids) {
            for (String s1 : picIDs) {
                if (Objects.equals(s, s1)) {
                    gonnaDelete.add(s);
                }
            }
        }

        ids.removeAll(gonnaDelete);

        Set<String> res = new TreeSet<>();
        res.addAll(ids);
        return res;
    }

    /**
     * ���������ļ������ѧ��ѧ��
     *
     * @param dir
     * @return
     */
    public Set<String> getOverSizeStudentIDS(String studentListFilename, String dir) {
        File[] files = new File(dir).listFiles();
        Set<String> strings = new TreeSet<>();
        if (files != null) {
            for (File file : files) {
                if (file.length() > 128 * 1024) {
                    strings.add(getStudentID(file.getName()));
                }
            }
        }
        return strings;
    }

    private String getStudentID(String fileName) {
        int end = 0;
        for (int i = 0; i < fileName.length(); i++) {
            if (fileName.charAt(i) == '_') {
                end = i;
            }
        }
        return fileName.substring(0, end);
    }
}