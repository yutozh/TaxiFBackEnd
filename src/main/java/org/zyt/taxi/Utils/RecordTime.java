package org.zyt.taxi.Utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class RecordTime {
    public static String def_path = "/home/whu/result/10000w-es-1119-3.txt";
    public static void writeLocalStrOne(String str, String path) {
        try {
            path = def_path;
            File file = new File(path);
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            file.createNewFile();
            if (str != null && !"".equals(str)) {
                FileWriter fw = new FileWriter(file, true);
                fw.write(str);//写入本地文件中
                fw.flush();
                fw.close();
//                System.out.println("执行完毕!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
