package org.apache.hive.hplsql.service.common.utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileUtils {
    public static File createFileNotExist(String filePath) throws IOException {
        File file = new File(filePath);
        if(file.getParentFile() != null){
            file.getParentFile().mkdirs();
        }
        file.createNewFile();
        return file;
    }

    /**
     * 获取某个目录下的所有文件名称集合（不含后缀名）
     * @param dirPath
     * @return
     */
    public static List<String> getFileNames(String dirPath){
        List<String> names = new ArrayList<>();
        File file = new File(dirPath);
        File[] files = file.listFiles();
        if(files == null){
            return names;
        }
        for (File f : files) {
            if(f.isFile()){
                names.add(f.getName().split("\\.")[0]);
            }
        }
        Collections.sort(names);
        return names;
    }

    /**
     * 在idea里运行返回target下classes文件夹的路径
     * 使用jar包执行时，返回jar包所在目录的路径
     * @return
     */
    public static String getClassesOrJarPath(){
        String path = FileUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if(path.contains(".jar")){
            path = path.substring(0, path.lastIndexOf(File.separator) + 1);
        }
        return path;
    }

}
