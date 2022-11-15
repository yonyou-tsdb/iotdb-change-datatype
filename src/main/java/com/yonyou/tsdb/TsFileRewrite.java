package com.yonyou.tsdb;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

public class TsFileRewrite {
    private static final Logger logger = LoggerFactory.getLogger(TsFileRewrite.class);
    static Map<Pattern, MetaEntity> patternMap = new HashMap<>();
    static Map<Path, MetaEntity> changedTimeSeriesMap = new HashMap<>();
    static List<Future<Map<Path, MetaEntity>>> futures = new ArrayList<>();

    private static String schemaDir;

    private static String patternConf;

    private final static int coreThreads = 10;

    public static void main(String[] args) throws IOException, InterruptedException {

        if (args.length < 2) {
            logger.warn("tsfile rewrite args empty, please input file dir, args :{} ", args);
            return;
        }

        String pathDir = args[0].trim();
        schemaDir = args[1].trim();
        if (args.length >= 3) patternConf = args[2].trim();
        File file = new File(pathDir);
        ExecutorService executor = Executors.newFixedThreadPool(coreThreads);
        // sequence
        List<File> seqFiles = FileListTools.getAllFiles(file);

        // init pattern
        initPattern();

        for (File f : seqFiles) {
            if (f.length() == 0) {
                continue;
            }
            Future<Map<Path, MetaEntity>> future = executor.submit(() -> {
                // replace
                String targetFilePath = f.getAbsolutePath().replace("data", "dataTmp");
                File targetFile = new File(targetFilePath);
                targetFile.getParentFile().mkdirs();
                TsFileRewritePerformer rewritePerformer = new TsFileRewritePerformer(f.getAbsolutePath(), targetFilePath);
                Map<Path, MetaEntity> map = new HashMap<>();
                try {
                    // rewrite tsfile
                    map = rewritePerformer.execute(patternMap, changedTimeSeriesMap);
                    // generate resource
                    FileLoaderUtils.checkTsFileResource(new TsFileResource(targetFile));
                } catch (IOException e) {
                    logger.error("error  path : " + f.getName() + " error :", e);
                }
                return map;
            });
            futures.add(future);
        }
        for (Future<Map<Path, MetaEntity>> future : futures) {
            try {
                Map<Path, MetaEntity> map = future.get();
                for (Map.Entry<Path, MetaEntity> entry : map.entrySet()) {
                    changedTimeSeriesMap.putIfAbsent(entry.getKey(), entry.getValue());
                }
            } catch (ExecutionException e) {
                logger.error("future get error : {}", e);
            }
        }
        executor.shutdown();
        // change schema
        for (Map.Entry<Path, MetaEntity> entry : changedTimeSeriesMap.entrySet()) {
            logger.info("path :" + entry.getKey().getFullPath()
                    + " dataType :" + entry.getValue().getDataType()
                    + " tsEncoding :" + entry.getValue().getTsEncoding()
                    + " compressionType :" + entry.getValue().getCompressionType());
        }
        if (!schemaDir.isEmpty()) {
            try {
                TsFileRewritePerformer.changeSchema(schemaDir, changedTimeSeriesMap);
            } catch (IllegalPathException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 解析配置文件，初始化匹配表达式
     */
    private static void initPattern() {

        // 解析配置文件
        File file = new File(patternConf);
        FileReader fr;
        try {
            fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                logger.info(line);
                String[] strs = line.split(" ");
                if (strs.length != 4) {
                    return;
                }
                MetaEntity entity = new MetaEntity(TSDataType.deserialize(Byte.parseByte(strs[1].trim())),
                        TSEncoding.deserialize(Byte.parseByte(strs[2].trim())),
                        CompressionType.deserialize(Byte.parseByte(strs[3].trim())));
                patternMap.put(Pattern.compile(strs[0].trim()), entity);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
