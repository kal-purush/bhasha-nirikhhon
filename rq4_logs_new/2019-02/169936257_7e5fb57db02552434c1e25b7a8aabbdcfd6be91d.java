import com.csvreader.CsvReader;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

public class JavaCSV {
    private static String CSVFileName;
    public static void main(String[] args) throws IOException {
//        args = new String[1];
        // 解析参数
        if (args.length!=1){
            System.out.println("参数不对，run.bat里面应为 java -jar JavaCSV.jar *.csv ");
            return;
        }
        CSVFileName = args[0];
//        CSVFileName = "C:\\Users\\xiaozhankai\\Desktop\\2018-10-29 15_32_00.csv";
        // 写一个map，用来存储对比用的标准的值
        Map map = new HashMap();
        File file = new File("map.txt");
        if (file.isFile()&&file.exists()){
            InputStreamReader readertxt = new InputStreamReader(new FileInputStream(file),"UTF-8");
            BufferedReader reader1 = new BufferedReader(readertxt);
            String lineTxt = null;
            while ((lineTxt=reader1.readLine())!=null){
                String[] split = lineTxt.split(",,,");
                map.put(split[0],split[1]);
            }
            readertxt.close();
        }else {
            System.out.println("找不到那个map.txt文件");
        }
//        map.put("总大肠菌群（MPN/100mL或CFU/100mL）",0);
//        map.put("耐热大肠菌群（MPN/100mL或CFU/101mL）",0);
//        map.put("大肠埃希氏菌（MPN/100mL或CFU/102mL）",0);
//        map.put("菌落总数（CFU/mL）",100);
//        map.put("砷（mg/L）",0.01);
//        map.put("镉（mg/L）",0.005);
//        map.put("铬（六价，mg/L）",0.05);
//        map.put("铅（mg/L）",0.01);
//        map.put("汞（mg/L）",0.001);
//        map.put("硒（mg/L）",0.01);
//        map.put("氰化物（mg/L）",0.05);
//        map.put("氟化物（mg/L）",1.0);
//        map.put("硝酸盐氮（以N计，mg/L）",10); // 百度为硝酸盐，地下水源限制时为20
//        map.put("三氯甲烷（mg/L）",0.06);
//        map.put("四氯化碳（mg/L）",0.002);
//        map.put("亚氯酸盐（使用二氧化氯消毒时，mg/L）",0.7); // 溴酸盐 甲醛 表格没有
//        map.put("氯酸盐（使用复合二氧化氯消毒时，mg/L）",0.7);
//        map.put("色度（铂钴色度单位）",15);
//        map.put("浑浊度（NTU-散射浊度单位）",1); // 水源与净水技术条件限制时为3
//        map.put("嗅和味",0);
//        map.put("肉眼可见物",0);
//        map.put("pH","6.5-8.5"); // 不小于6.5且不大于8.5
//        map.put("铝（mg/L）",0.2);
//        map.put("铁（mg/L）",0.3);
//        map.put("锰（mg/L）",0.1);
//        map.put("铜（mg/L）",1.0);
//        map.put("锌（mg/L）",1.0);
//        map.put("氯化物（mg/L）",250);
//        map.put("硫酸盐（mg/L）",250);
//        map.put("溶解性总固体（mg/L）",1000);
//        map.put("总硬度(以CaCO3计，mg/L）",450);
//        map.put("耗氧量（CODMn法，以O2计，mg/L）",3); // 水源限制，原水耗氧量>6mg/L时为5
//        map.put("挥发酚类（以苯酚计，mg/L）",0.002);
//        map.put("阴离子合成洗涤剂（mg/L）",0.3);
//        map.put("氯消毒为游离余氯（mg/L）",0.05); // 以末梢水为准
//        map.put("二氧化氯",0.02);
//        map.put("氨氮（mg/L）",0.5);
//        map.put("总β放射性（Bq/L）",1);
//        map.put("锑（mg/L）",0.005);
//        map.put("钡（mg/L）",0.7);
//        map.put("铍（mg/L）",0.002);
//        map.put("硼（mg/L）",0.5);
//        map.put("钼（mg/L）",0.07);
//        map.put("镍（mg/L）",0.02);
//        map.put("银（mg/L）",0.05);
//        map.put("铊（mg/L）",0.0001);
//        map.put("氯化氰（mg/L）",0.07);
//        map.put("一氯二溴甲烷（mg/L）",0.1);
//        map.put("二氯一溴甲烷（mg/L）",0.06);
//        map.put("二氯乙酸（mg/L）",0.05);
//        map.put("1,2-二氯乙烷（mg/L）",0.03);
//        map.put("二氯甲烷（mg/L）",0.02);
//        map.put("1,1,1-三氯乙烷（mg/L）",2);
//        map.put("三氯乙酸（mg/L）",0.1);
//        map.put("三氯乙醛（mg/L）",0.01);
//        map.put("2,4,6-三氯酚（mg/L）",0.2);
//        map.put("三溴甲烷（mg/L）",0.1);
//        map.put("七氯（mg/L）",0.0004);
//        map.put("马拉硫磷（mg/L）",0.25);
//        map.put("五氯酚（mg/L）",0.009);
//        map.put("六六六（mg/L）",0.005);
//        map.put("六氯苯（mg/L）",0.001);
//        map.put("乐果（mg/L）",0.08);
//        map.put("对硫磷（mg/L）",0.003);
//        map.put("灭草松（mg/L）",0.3);
//        map.put("甲基对硫磷（mg/L）",0.02);
//        map.put("百菌清（mg/L）",0.01);
//        map.put("呋喃丹（mg/L）",0.007);
//        map.put("林丹（γ-666）（mg/L）",0.002); // 两个括号啊
//        map.put("毒死蜱（mg/L）",0.03);
//        map.put("草甘膦（mg/L）",0.7);
//        map.put("敌敌畏（mg/L）",0.001);
//        map.put("莠去津（mg/L）",0.002);
//        map.put("溴氰菊酯（mg/L）",0.02);
//        map.put("2,4-滴（mg/L）",0.03);
//        map.put("滴滴涕（mg/L）",0.001);
//        map.put("乙苯（mg/L）",0.3);
//        map.put("二甲苯（mg/L）",0.5);
//        map.put("1,1-二氯乙烯（mg/L）",0.03);
//        map.put("1,2-二氯乙烯（mg/L）",0.05);
//        map.put("1,2-二氯苯（mg/L）",1);
//        map.put("1,4-二氯苯（mg/L）",0.3);
//        map.put("三氯乙烯（mg/L）",0.07);
//        map.put("三氯苯（mg/L）",0.02);
//        map.put("六氯丁二烯（mg/L）",0.0006);
//        map.put("丙烯酰胺（mg/L）",0.0005);
//        map.put("四氯乙烯（mg/L）",0.04);
//        map.put("甲苯（mg/L）",0.7);
//        map.put("邻苯二甲酸二(2-乙基已基)酯（mg/L）",0.008);
//        map.put("环氧氯丙烷（mg/L）",0.0004);
//        map.put("苯（mg/L）",0.01);
//        map.put("苯乙烯（mg/L）",0.02);
//        map.put("苯并(a)芘（mg/L）",0.00001); // 带括号
//        map.put("氯乙烯（mg/L）",0.005);
//        map.put("氯苯（mg/L）",0.3);
//        map.put("微囊藻毒素（mg/L）",0.001);
//        map.put("硫化物（mg/L）",0.02);
//        map.put("钠（mg/L）",200);
//        map.put("总α放射性（Bq/L）",0.5);
//        map.put("贾第鞭毛虫（个/10L）",1); // 现在是小于1
//        map.put("隐孢子虫（个/10L）",1);
//        map.put("",0);
//        map.put("",0);
//        map.put("",0);
//        map.put("",0);



        // 写一个map，用来存储最后的key value，类似区名称：建华，不合格项目：锰，数值：12(key不能重复，凉了)

        // 写一个List<Map<String,String>>用来存值
        List<Map<String,String>> mapList = new ArrayList<Map<String, String>>();
        // 输出key，value
//        for (Object key:map.keySet()){
//            System.out.println("key="+key+"value="+map.get(key));
//        }
        // 读取CSV文件
        CsvReader reader = new CsvReader(CSVFileName,',', Charset.forName("GBK"));
        // 读取表头
        reader.readHeaders(); // 很奇怪的啊，必须读取表头，不读取没得数据，想起来了好像javacsv里面的方法表头和数据分开的
        // 读值判断
        while (reader.readRecord()){
            for (Object key:map.keySet()){
                // 先判断是否为“”,是这个的话不能强转为double
                if (!reader.get(key.toString()).equals("")){
                    // 特殊的三个幺儿要提到前面来
                    if (key.equals("氯消毒为游离余氯（mg/L）")){
                        // 如果小于标准值，不合格
                        if (Double.parseDouble(reader.get(key.toString()))<Double.parseDouble(map.get(key).toString())){
                            Map map1 = new HashMap();
                            map1.put("区县名",reader.get("区县名"));
                            map1.put("报告编号",reader.get("样品编号"));
                            map1.put("采样地点",reader.get("供水单位详细地点"));
                            map1.put("水样类型",reader.get("水样类型")); // 后加入
                            map1.put("检测日期",reader.get("测定日期"));
                            map1.put("不合格检验项目/单位",key);
                            map1.put("限值",map.get(key).toString());
                            map1.put("检测结果",reader.get(key.toString()));
                            mapList.add(map1);
                        }
                    }else if (key.equals("二氧化氯")){
                        // 如果小于标准值，不合格
                        if (Double.parseDouble(reader.get(key.toString()))<Double.parseDouble(map.get(key).toString())){
                            Map map1 = new HashMap();
                            map1.put("区县名",reader.get("区县名"));
                            map1.put("报告编号",reader.get("样品编号"));
                            map1.put("采样地点",reader.get("供水单位详细地点"));
                            map1.put("水样类型",reader.get("水样类型")); // 后加入
                            map1.put("检测日期",reader.get("测定日期"));
                            map1.put("不合格检验项目/单位",key);
                            map1.put("限值",map.get(key).toString());
                            map1.put("检测结果",reader.get(key.toString()));
                            mapList.add(map1);
                        }
                    }else if (key.equals("pH")){
                        if (Double.parseDouble(reader.get(key.toString()))<6.5||Double.parseDouble(reader.get(key.toString()))>8.5){
                            Map map1 = new HashMap();
                            map1.put("区县名",reader.get("区县名"));
                            map1.put("报告编号",reader.get("样品编号"));
                            map1.put("采样地点",reader.get("供水单位详细地点"));
                            map1.put("水样类型",reader.get("水样类型")); // 后加入
                            map1.put("检测日期",reader.get("测定日期"));
                            map1.put("不合格检验项目/单位",key);
                            map1.put("限值",map.get(key).toString());
                            map1.put("检测结果",reader.get(key.toString()));
                            mapList.add(map1);
                        }
                    }
                    // 读取到的数值和标准数值进行一个判断,如果大于标准值，不合格
                    else if (Double.parseDouble(reader.get(key.toString()))>Double.parseDouble(map.get(key).toString())){
                        Map map1 = new HashMap();
                        map1.put("区县名",reader.get("区县名"));
                        map1.put("报告编号",reader.get("样品编号"));
                        map1.put("采样地点",reader.get("供水单位详细地点"));
                        map1.put("水样类型",reader.get("水样类型")); // 后加入
                        map1.put("检测日期",reader.get("测定日期"));
                        map1.put("不合格检验项目/单位",key);
                        map1.put("限值",map.get(key).toString());
                        map1.put("检测结果",reader.get(key.toString()));
                        mapList.add(map1);
//                        System.out.println("超标项目为："+key+"超标的值为"+reader.get(key.toString())+"标准值为："+map.get(key));
                    }
                }
            }
        }

        for (Map m:mapList){
            for (Object k:m.keySet()){
                System.out.print(m.get(k)+";");
            }
            System.out.println();
        }

        // 导出到excel
        HSSFWorkbook workbook = new HSSFWorkbook(); // 创建工作簿对象
        HSSFSheet sheet = workbook.createSheet("不合格数据"); // 创建第一个sheet页
        sheet.autoSizeColumn(1,true);
        sheet.setColumnWidth(0, 1840);
        HSSFRow row1 = sheet.createRow(0); // 创建行
//        row.setHeightInPoints(30); // 设置行高
        HSSFCell cell1 = row1.createCell(0); // 创建单元格
        cell1.setCellValue("区县名"); // 设置值
        HSSFCell cell2 = row1.createCell(1); // 创建单元格
        cell2.setCellValue("报告编号"); // 设置值
        HSSFCell cell3 = row1.createCell(2); // 创建单元格
        cell3.setCellValue("采样地点"); // 设置值
        HSSFCell cell4 = row1.createCell(3); // 创建单元格
        cell4.setCellValue("水样类型"); // 设置值
        HSSFCell cell5 = row1.createCell(4); // 创建单元格
        cell5.setCellValue("检测日期"); // 设置值
        HSSFCell cell6 = row1.createCell(5); // 创建单元格
        cell6.setCellValue("不合格检验项目"); // 设置值
        HSSFCell cell7 = row1.createCell(6); // 创建单元格
        cell7.setCellValue("单位"); // 设置值
        HSSFCell cell8 = row1.createCell(7); // 创建单元格
        cell8.setCellValue("限值"); // 设置值
        HSSFCell cell9 = row1.createCell(8); // 创建单元格
        cell9.setCellValue("检测结果"); // 设置值
        // 输入表格的具体值
        for (int i=0;i<mapList.size();i++){
            // 这里因为第一行要写表头，所以从第二行开始创建
            HSSFRow row = sheet.createRow(i+1); // 创建行
            for (int j=0;j<mapList.get(i).size()+1;j++){
                HSSFCell cell = row.createCell(j); // 创建单元格
                switch (j){
                    case 0:
                        cell.setCellValue(mapList.get(i).get("区县名"));
                        break;
                    case 1:
                        cell.setCellValue(mapList.get(i).get("报告编号"));
                        break;
                    case 2:
                        cell.setCellValue(mapList.get(i).get("采样地点"));
                        break;
                    case 3:
                        cell.setCellValue(mapList.get(i).get("水样类型"));
                        break;
                    case 4:
                        String[] size = mapList.get(i).get("检测日期").split(" ");
                        cell.setCellValue(mapList.get(i).get("检测日期").substring(0,size[0].length()));
                        break;
                    case 5:
                        // 截取前面的单位
                        String[] danwei = mapList.get(i).get("不合格检验项目/单位").split("（");
                        cell.setCellValue(mapList.get(i).get("不合格检验项目/单位").substring(0,danwei[0].length()));
                        break;
                    case 6:
                        // 这里后来报错的原因啊！是有的没有单位不能分割，有的（写错了，哼
                        String[] danwei2 = mapList.get(i).get("不合格检验项目/单位").split("（");
                        cell.setCellValue(mapList.get(i).get("不合格检验项目/单位").substring(danwei2[0].length()).replace("（","").replace("）",""));

                        break;
                    case 7:
//                        if ()
                        cell.setCellValue(mapList.get(i).get("限值"));
                        break;
                    case 8:
                        cell.setCellValue(mapList.get(i).get("检测结果"));
                        break;
                }
            }
        }
//        FileOutputStream stream = new FileOutputStream("C:\\Users\\xiaozhankai\\Desktop\\2018-10-29 15_32_00_不合格.xls");
        FileOutputStream stream = new FileOutputStream(CSVFileName+"不合格.xls");
        workbook.write(stream);
        stream.close();


        reader.close();
    }
}