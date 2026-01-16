import java.io.*;
import java.util.Arrays;

public class ioFile {
    BufferedReader bufferedReader;
    BufferedWriter bufferedWriter;
    String pathRead, pathWrite;

    public static void main(String[] args) {
        //new ioFile("A:\\Java\\Java project\\hexTEST\\out\\production\\hexTEST\\ShortMsgKAT.txt", "").find("# Principal Submitter:");

    }

    public ioFile() {
    }

    public boolean setPathRead(String pathRead) {
        this.pathRead = pathRead;
        return openFileForReadFile();
    }

    public boolean setPathWrite(String pathWrite) {
        this.pathWrite = pathWrite;
        return openFileForWriteFile();
    }

    private boolean openFileForReadFile() {
        File file = new File(pathRead);
        try {
            bufferedReader = new BufferedReader(new FileReader(file));
            return true;
        } catch (FileNotFoundException e) {
            System.out.format("Couldn't open <%s> for read\n", file.getName());
            return false;
        }
    }

    private boolean openFileForWriteFile() {
        File file = new File(pathWrite);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file);
            bufferedWriter = new BufferedWriter(fw);
            return true;
        } catch (IOException ioe) {
            System.out.format("Couldn't open <%s> for write\n", file.getName());
            return false;
        }
    }

    public String find(String tample) {
        String line;
        try {
            while ((line = bufferedReader.readLine()) != null) {
                if (line.indexOf(tample) != -1) {
                    return line.substring(tample.length());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void writeToFile(String line, Object param) {
        try {
            bufferedWriter.write(String.format(line, param));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeBToFile(String line, String param, int L) {
        writeToFile("%s", line);
        char A[] = param.toCharArray();

        for (int i = 0; i < L; i++)
            writeToFile("%02X", A[i]);

        if (L == 0)
            writeToFile("00", "");

        writeToFile("\n", "");
    }

    public boolean ReadHEX(String s, int Length, String str) {
        int ch;
        boolean started;
        char ich = 0;
        char A[] = s.toCharArray();

        if (Length == 0) {
            A[0] = 0x00;
            return true;
        }

        Arrays.fill(A, 0, Length, (char) 0x00);

        started = false;
        if (find(str) != null) {
            try {
                while ((ch = bufferedReader.read()) != -1) {
                    if (!Character.isDigit(ch)) {
                        if (!started) {
                            if (ch == '\n')
                                break;
                            else
                                continue;
                        } else
                            break;
                    }
                    started = true;
                    if ((ch >= '0') && (ch <= '9'))
                        ich = (char) (ch - '0');
                    else if ((ch >= 'A') && (ch <= 'F'))
                        ich = (char) (ch - 'A' + 10);
                    else if ((ch >= 'a') && (ch <= 'f'))
                        ich = (char) (ch - 'a' + 10);

                    for (int i = 0; i < Length - 1; i++)
                        A[i] = (char) ((A[i] << 4) | (A[i + 1] >> 4));
                    A[Length - 1] = (char) ((A[Length - 1] << 4) | ich);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            return false;
        }
        return true;

    }

    public boolean closeFiles() {
        try {
            if (bufferedWriter != null)
                bufferedWriter.close();
            if (bufferedReader != null)
                bufferedReader.close();
            return true;
        } catch (Exception ex) {
            System.out.println("Error in closing" + ex);
            return false;
        }
    }
}