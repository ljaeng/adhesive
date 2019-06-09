package com.jaeng.adhesive.common.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public class PackageUtil {

    private static List<String> getClassName(String filePath, List<String> className) {
        List<String> classNameList = new ArrayList<>();

        File file = new File(filePath);
        File[] childFiles = file.listFiles();
        for (File childFile : childFiles) {
            if (childFile.isDirectory()) {
                classNameList.addAll(getClassName(childFile.getPath(), classNameList));
            } else {
                String childFilePath = childFile.getPath();
                childFilePath = childFilePath.substring(childFilePath.indexOf("\\classes") + 9, childFilePath.lastIndexOf("."));
                childFilePath = childFilePath.replace("\\", ".");
                classNameList.add(childFilePath);
            }
        }

        return classNameList;
    }


    public static List<String> getClassName(String packageName) {
        String filePath = ClassLoader.getSystemResource("").getPath() + packageName.replace(".", "\\");
        List<String> fileNames = getClassName(filePath, null);
        return fileNames;
    }

    public static void main(String[] args) {
        String packageName = "com.jaeng.adhesive.common.util";

        List<String> classNames = getClassName(packageName);
        for (String className : classNames) {
            System.out.println(className);
        }
    }
}
