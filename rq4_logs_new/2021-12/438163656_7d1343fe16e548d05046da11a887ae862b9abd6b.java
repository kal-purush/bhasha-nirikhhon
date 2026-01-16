import javax.swing.*;
import javax.swing.filechooser.FileSystemView;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author AL
 * @date 2021/12/11 17:58
 */
public class GenerateVueID extends JFrame implements ActionListener {

    public static Integer count = 0;

    JButton open = null;

    /**
     * 初始化选择框
     */
    public GenerateVueID() {
        open = new JButton("请选择包含vue的文件夹");
        this.add(open);
        this.setBounds(400, 200, 1000, 1000);
        this.setVisible(true);
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        open.addActionListener(this);
    }

    /**
     * 选择要处理的文件夹
     * @param e
     */
    @Override
    public void actionPerformed(ActionEvent e) {
        //文件选择器获取文件或者文件夹
        JFileChooser jfc = new JFileChooser();
        //设置当前路径为桌面路径,否则将我的文档作为默认路径
        FileSystemView fsv = FileSystemView.getFileSystemView();
        jfc.setCurrentDirectory(fsv.getHomeDirectory());
        //JFileChooser.FILES_AND_DIRECTORIES 选择路径和文件
        jfc.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
        //用户选择的路径或文件
        if (jfc.showOpenDialog(GenerateVueID.this) == JFileChooser.APPROVE_OPTION) {
            File file = jfc.getSelectedFile();
            if (file.isDirectory()) {
                System.out.println("您选择了处理的文件夹:" + file.getAbsolutePath());
                // 遍历此文件夹的vue文件
                findVueFileInDir(file);
            }
        }

        //文件选择器获取文件,这里只能获取文件，不能获取文件夹
       /* JFileChooser jfc=new JFileChooser("C:\\");//可以直接在这设置默认路径
        if(jfc.showOpenDialog(bbbbbbbbbb.this)==JFileChooser.APPROVE_OPTION){
        File file=jfc.getSelectedFile();
              System.out.println("文件:"+file.getAbsolutePath());
        }*/

    }

    /**
     * 筛选出只有vue结尾的文件
     * @param dirFile
     */
    public void findVueFileInDir(File dirFile) {
        // 遍历此文件夹的vue文件
        File[] files = dirFile.listFiles();
        for (File f : files) {
            if (f.isFile() && f.getName().endsWith(".vue")) {
                parseVueFile(f);
            } else if (f.isDirectory()) {
                findVueFileInDir(f);
            }
        }
    }

    /**
     * 解析vue文件，给vue按钮添加id属性
     * @param file
     */
    public void parseVueFile(File file) {

        System.err.println("开始处理文件："+file.getAbsolutePath());
        // 解析文件内容 该文件添加id的个数计数
        int num = 0;
        try {
            File tempFile = File.createTempFile("vueTemp", ".vue", file.getParentFile());

            // id前缀： vue文件所在的文件夹名-vue文件名
            String vueFilenameWithSuffix = file.getName();
            String vueFileName = vueFilenameWithSuffix.substring(0, vueFilenameWithSuffix.indexOf("."));
            String idPrefix = file.getParentFile().getName() + "-" + vueFileName;

            BufferedReader br = null;
            BufferedWriter bw = null;

            try {
                br = new BufferedReader(new FileReader(file));
                String line = null;
                bw = new BufferedWriter(new FileWriter(tempFile));
                while ((line = br.readLine()) != null) {
                    if (!line.equals("") && line.contains("<el-button")) {

                        // 所有vue文件添加id属性的个数
                        count++;

                        // 添加id属性
                        Map<String, Object> data = getNewLineByReplace(line, idPrefix, num);
                        line = data.get("newLine").toString();
                        num = Integer.valueOf(data.get("num").toString());
                        num++;
                    }
                    // 写入到新文件
                    bw.write(line+"\n");
                }
                bw.flush();
            }finally {
                if (br!=null){
                    try {
                        br.close();
                    } catch (IOException e) {

                    }
                }
                if (bw!=null){
                    try {
                        bw.close();
                    }catch (IOException e){

                    }
                }

            }

            // 先写入临时文件，再替换成源文件
            file.delete();
            tempFile.renameTo(file);
            tempFile.delete();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("结束处理文件："+file.getAbsolutePath()+"--添加了【"+num+"】个id属性元素");
    }

    private Map<String,Object> getNewLineByReplace(String line, String idPrefix, int beginNum) {
        String domId = idPrefix + "-" + beginNum;
        line = line.replace("<el-button", "<el-button id=\"" + domId + "\"");
        Map<String,Object> map = new HashMap<>(16);
        map.put("newLine",line);
        map.put("num",beginNum);
        return map;
    }


    /**
     *
     * 对元素进行编号
     * 正在方式处理
     * @param line
     * @param idPrefix
     * @param beginNum
     * @return
     */
    private static String getNewLineByRegex(String line, String idPrefix,int beginNum) {

        Matcher m = pattern.matcher(line);
        StringBuffer result = new StringBuffer();

        while(m.find()){
            String idAttribute = " id=\""+idPrefix+"-"+beginNum+"\" ";
            String replaceTo = m.group(1)+idAttribute;
            System.err.println("result:"+beginNum+":"+result);
            m.appendReplacement(result,replaceTo);
            beginNum++;
        }
        m.appendTail(result);
        return result.toString();
    }


    /**
     * 配置需要给元素编id
     */
    static String[] strings = new String[]{"<el-button","<el-cc"};
    public static String patternStr;

    static {
        StringBuffer bf = new StringBuffer("(");
        for (String string : strings) {
            bf.append(string).append("|");
        }
        String substring = bf.toString().substring(0, bf.length() - 1);
        patternStr = substring+")";
    }

    public static Pattern pattern = Pattern.compile(patternStr);

    /**
     * 主入口
     * @param args
     */

    public static void main(String[] args) {
        new GenerateVueID();
//        String line = "<el-button class='1' /el-button> <el-button /el-button> <el-cc /el-cc>";
//        String newLineByRegex = getNewLineByRegex(line, "vue-id", 0);
//        System.err.println(newLineByRegex);
    }
}
