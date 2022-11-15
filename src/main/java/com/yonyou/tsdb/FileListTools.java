package com.yonyou.tsdb;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileListTools {

    public static List<File> getAllFiles(File dir) {
        File[] files = dir.listFiles(new FileFilterImpl());
        List<File> fileList = new ArrayList<>();
        for (File file : files) {
            if (file.isDirectory()) {
                fileList.addAll(getAllFiles(file));
            } else {
                fileList.add(file);
            }
        }
        return fileList;
    }
}
