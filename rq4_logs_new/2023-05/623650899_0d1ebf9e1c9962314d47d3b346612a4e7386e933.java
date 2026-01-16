package Steps;

import DataTypes.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class ZipperStep extends Step{
    private StringType source;
    private EnumeratorType operation;
    private StringType result;
    private static double stepAvgDuration=0;
    private static int stepStartUpCount=0;

    public ZipperStep(){
        super("Zipper", false);

        this.source = new StringType(StepInputNameEnum.SOURCE.toString(), true);
        this.operation=new EnumeratorType(StepInputNameEnum.OPERATION.toString(), true);
        this.result=new StringType(StepOutputNameEnum.RESULT.toString(),false);
        this.source.setMandatory(true);
        this.operation.setMandatory(true);
    }

    public ZipperStep(StringType source, EnumeratorType operation) {
        this();
        this.source = source;
        this.operation=operation;
        this.source.setMandatory(true);
        this.operation.setMandatory(true);
    }

    @Override
    protected void outerRunStepFlow(){
        try {
            this.runStepFlow();
        } catch (ZipException e){
            this.setSummaryLine(e.getMessage());
            this.setStatus(Status.Failure);
            this.addLog(e.getMessage());
            this.result.setData("Failure");
        }
        catch (Exception e) {
            this.setSummaryLine("Exception: " + e.getMessage());
            this.setStatus(Status.Failure);
        }

    }

    @Override
    protected void runStepFlow() throws Exception {
        String path=source.getData();
        ZipOperation op=ZipOperation.valueOf(operation.getData());

        addLog("About to perform operation "+op.toString()+" on source "+path);
        if(!(new File(path).exists()))throw new ZipException("The source provided does not exit");
        if(op==ZipOperation.ZIP && path.endsWith("zip"))throw new ZipException("Attempting to zip a zip file");
        if(op==ZipOperation.UNZIP && !path.endsWith("zip"))throw new ZipException("Attempting to unzip a non zip file");
        if(op==ZipOperation.ZIP)
            compress(path);
        else
            decompress(path);
        this.result.setData("Success");
    }



    private void compress(String path) throws Exception{
        File file =new File(path);
        String[] strings=new String[1];
        strings[0]=path;
        if(file.isDirectory())
            new ZipUsingJavaUtil().zip(strings, path.substring(0, path.lastIndexOf('\\'))+".zip");
        else
            new ZipUsingJavaUtil().zip(strings, path.substring(0, path.lastIndexOf('.'))+".zip");
    }

    private void decompress(String zipName) throws Exception{
        byte[] buffer = new byte[2048];
        Path outDir = Paths.get(zipName.substring(0,zipName.lastIndexOf('\\'))+"\\");

        try (ZipInputStream stream = new ZipInputStream(new BufferedInputStream(new FileInputStream(zipName)))) {

            ZipEntry entry;
            while ((entry = stream.getNextEntry()) != null) {

                Path filePath = outDir.resolve(entry.getName());
                if(!entry.isDirectory()) {
                    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath.toFile()), buffer.length)) {
                        int len;
                        while ((len = stream.read(buffer)) > 0) {
                            bos.write(buffer, 0, len);
                        }
                    }
                }
                else{
                    File dir= new File(filePath.toString());
                    dir.mkdirs();
                }
            }
        }
    }

    private enum ZipOperation{
        ZIP,
        UNZIP
    }

    @Override
    public void setInputs(DataType... inputs) {
        for(DataType input: inputs){
            if(input.getEffectiveName().equals(source.getEffectiveName())) {
                this.source.setData((String) input.getData());
                this.source.setMandatory(true);
            }
            if(input.getEffectiveName().equals(operation.getEffectiveName())) {
                this.operation.setData((String) input.getData());
                this.operation.setMandatory(true);
            }
        }
    }

    @Override
    public void setInputByName(DataType input, String inputName) {
        if(inputName.equals(source.getEffectiveName()))
            this.source.setData((String) input.getData());
        if(inputName.equals(operation.getEffectiveName()))
            this.operation.setData((String) input.getData());
    }

    @Override
    public ArrayList<DataType> getOutputs(String... outputNames) {
        ArrayList<DataType> outputsArray=new ArrayList<>();
        for(String outputName: outputNames){
            if(this.result.getEffectiveName().equals(outputName))
                outputsArray.add(this.result);
        }
        return outputsArray;
    }

    @Override
    public ArrayList<DataType> getAllData() {
        ArrayList<DataType> allData=new ArrayList<>();
        allData.add(this.source);
        allData.add(this.operation);
        allData.add(this.result);
        return  allData;
    }



    @Override
    protected void updateStaticTimers() {
        stepStartUpCount= startUpCounter;
        stepAvgDuration=durationAvgInMs;
    }

    public class ZipException extends Exception{
        public ZipException(String str){
            super(str);
        }
    }


    public static int getStepStartUpCount() {
        return stepStartUpCount;
    }

    public static double getStepAvgDuration() {
        return stepAvgDuration;
    }

    public void clearDataMembers(){
        this.source.eraseData();
        this.operation.eraseData();
        this.result.eraseData();
    }

    private static class ZipUsingJavaUtil {
        /**
         * A constants for buffer size used to read/write data
         */
        private static final int BUFFER_SIZE = 4096;
        /**
         * Compresses a list of files to a destination zip file
         * @param listFiles A collection of files and directories
         * @param destZipFile The path of the destination zip file

         */
        public void zip(List<File> listFiles, String destZipFile) throws FileNotFoundException,
                IOException {
            ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(destZipFile));
            for (File file : listFiles) {
                if (file.isDirectory()) {
                    zipDirectory(file, file.getName(), zos);
                } else {
                    zipFile(file, zos);
                }
            }
            zos.flush();
            zos.close();
        }
        /**
         * Compresses files represented in an array of paths
         * @param files a String array containing file paths
         * @param destZipFile The path of the destination zip file
         * @throws FileNotFoundException
         * @throws IOException
         */
        public void zip(String[] files, String destZipFile) throws FileNotFoundException, IOException {
            List<File> listFiles = new ArrayList<File>();
            for (int i = 0; i < files.length; i++) {
                listFiles.add(new File(files[i]));
            }
            zip(listFiles, destZipFile);
        }
        /**
         * Adds a directory to the current zip output stream
         * @param folder the directory to be  added
         * @param parentFolder the path of parent directory
         * @param zos the current zip output stream
         * @throws FileNotFoundException
         * @throws IOException
         */
        private void zipDirectory(File folder, String parentFolder, ZipOutputStream zos) throws FileNotFoundException, IOException {
            zos.putNextEntry(new ZipEntry(parentFolder+"/" ));
            for (File file : folder.listFiles()) {
                if (file.isDirectory()) {
                    zipDirectory(file, parentFolder + "/" + file.getName(), zos);
                    continue;
                }
                zos.putNextEntry(new ZipEntry(parentFolder + "/" + file.getName()));
                BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
                long bytesRead = 0;
                byte[] bytesIn = new byte[BUFFER_SIZE];
                int read = 0;
                while ((read = bis.read(bytesIn)) != -1) {
                    zos.write(bytesIn, 0, read);
                    bytesRead += read;
                }

                zos.closeEntry();
            }
        }
        /**
         * Adds a file to the current zip output stream
         * @param file the file to be added
         * @param zos the current zip output stream
         * @throws FileNotFoundException
         * @throws IOException
         */
        private void zipFile(File file, ZipOutputStream zos) throws FileNotFoundException, IOException {
            zos.putNextEntry(new ZipEntry(file.getName()));
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
            long bytesRead = 0;
            byte[] bytesIn = new byte[BUFFER_SIZE];
            int read = 0;
            while ((read = bis.read(bytesIn)) != -1) {
                zos.write(bytesIn, 0, read);
                bytesRead += read;
            }

            zos.closeEntry();
        }
    }
}