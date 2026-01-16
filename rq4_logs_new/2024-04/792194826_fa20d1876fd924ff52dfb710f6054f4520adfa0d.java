package com.zyh.Command;

import com.zyh.Constant.SysConstant;
import com.zyh.Exception.SysException;
import com.zyh.Store.Blob;
import com.zyh.Store.Entry;
import com.zyh.Store.Index;
import com.zyh.Utils.FileUtils;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.*;

@Data
public class AddCommand implements Command{

    public static final Logger LOGGER = LoggerFactory.getLogger(AddCommand.class);

    private String userWorkspace;

    private String sysWorkspace;

    private Index index;

    private Map<String,Entry> updateData;

    // 记录找到的文件
    private Set<String> existFiles;

    public AddCommand(String userWorkspace){
        this.userWorkspace = userWorkspace;
        this.sysWorkspace = FileUtils.generateFilePath(userWorkspace,SysConstant.WORK_ROOT_DIR,SysConstant.WORK_OBJECTS_DIR);
        try {
            index = Index.generateIndexFile(FileUtils.generateFilePath(userWorkspace, SysConstant.WORK_ROOT_DIR));
        } catch (SysException e){
            LOGGER.error("[AddCommand] 初始化失败",e);
        }
        updateData = new TreeMap<>();
        existFiles = new HashSet<>();
    }

    public void add() throws SysException {
        File file = new File(userWorkspace);
        searchFile(file);
        // 标记被删除的文件
        index.batchSetDelete(existFiles);
    }

    public void searchFile(File file) throws SysException {
        if(!file.isDirectory()){
            throw new SysException("[searchFile] 入参必须为目录");
        }
        File[] files = file.listFiles();

        // 查找存在的文件
        for (int i = 0; i < files.length; i++) {
            String name = files[i].getName();
            if(FileUtils.filter(name)){
                continue;
            }
            if(files[i].isDirectory()){
                searchFile(files[i]);
            } else {
                String path = generateRelativePath(files[i]);
                existFiles.add(path);
                Blob blob = new Blob(files[i],null);
                // 压缩成文件
                blob.writeToFile(sysWorkspace);
                if(blob.getUpdated()){
                    updateData.put(path, new Entry(blob.getHashByFileName(), path,false));
                }
            }
        }
    }



    @Override
    public void exec(String[] args) {
        try {
            // 添加所有
            if (args.length == 1 || (args.length == 2 && args[1].equals("."))) {
                add();
            } else {
                HashSet<String> fileNamesSet = new HashSet<>();
                for (int i = 1; i < args.length; i++) {
                    fileNamesSet.add(args[i]);
                }
                addFiles(fileNamesSet);
            }

            // 更新Index
            index.update(updateData);
            index.writeToFile(null);
        } catch (SysException e){
            LOGGER.error(e.toString());
        }
    }

    private void addFiles(HashSet<String> s) {
        s.stream().forEach(fileName -> {
            try {
                File file = new File(FileUtils.generateFilePath(userWorkspace, fileName));
                String path = generateRelativePath(file);
                if (file.exists()) {
                    if (file.isFile()) {
                        Blob blob = new Blob(file, null);
                        blob.writeToFile(sysWorkspace);
                        if(blob.getUpdated()){
                            updateData.put(path, new Entry(blob.getHashByFileName(), path,false));
                        }
                    } else if (file.isDirectory()) {
                        searchFile(file);
                    }
                } else if(index.containsFile(path)){
                    // 文件不存在，即删除或移动了该文件
                    index.setDelete(path);
                }
            } catch (SysException e){
                LOGGER.error(e.toString());
            }
        });
    }

    private String generateRelativePath(File file){
        File baseFile = new File(userWorkspace);
        Path basePath = baseFile.toPath();
        Path path = file.toPath();
        Path relativize = basePath.relativize(path);

        return relativize.toString();
    }
}