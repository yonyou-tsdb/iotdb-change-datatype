package com.yonyou.tsdb;

import java.io.File;
import java.io.FileFilter;

public class FileFilterImpl  implements FileFilter {

    private final String fileSuffix = ".tsfile";
    @Override
    public boolean accept(File pathname) {
        if (pathname.isDirectory()) {
            return true;
        }
        return pathname.getName().toLowerCase().endsWith(fileSuffix);
    }
}
