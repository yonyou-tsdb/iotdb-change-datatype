/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.yonyou.tsdb;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileAlignedSeriesReaderIterator;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * This class is used to rewrite one tsfile with current dataType & encoding & compressionType.
 */
public class TsFileRewritePerformer {

    private static final Logger logger = LoggerFactory.getLogger(TsFileRewritePerformer.class);

    private final String tsFilePath;

    private final String targetTsFilePath;

    private final Map<Path, MetaEntity> changedMap = new HashMap<>();

    public TsFileRewritePerformer(String tsFilePath, String targetTsFilePath) {
        this.tsFilePath = tsFilePath;
        this.targetTsFilePath = targetTsFilePath;
    }

    public static void changeSchema(String schemaDir, Map<Path, MetaEntity> changedMap) throws IllegalPathException, IOException {
        logger.info("start change schema : {}", schemaDir);
        MLogWriter logWriter = new MLogWriter(schemaDir, MetadataConstant.METADATA_LOG);
        for (Map.Entry<Path, MetaEntity> entry : changedMap.entrySet()) {
            PartialPath partialPath = new PartialPath(entry.getKey().getFullPath());
            DeleteTimeSeriesPlan deleteTimeSeriesPlan = new DeleteTimeSeriesPlan();
            deleteTimeSeriesPlan.setPaths(Lists.newArrayList(partialPath));
            logWriter.deleteTimeseries(deleteTimeSeriesPlan);
            logger.info("delete time series plan : {}", deleteTimeSeriesPlan);
            MetaEntity entity = entry.getValue();
            CreateTimeSeriesPlan plan = new CreateTimeSeriesPlan(partialPath, entity.getDataType(), entity.getTsEncoding(), entity.getCompressionType(), null, null, null, null);
            logWriter.createTimeseries(plan);
            logger.info("create time series plan result : {}", plan);
        }
        logWriter.close();
    }
    /**
     * init need changed path
     * @param patternMap
     * @param changedTimeSeriesMap
     */
    private void init(Map<Pattern, MetaEntity> patternMap, Map<Path, MetaEntity> changedTimeSeriesMap) {
        TsFileSequenceReader reader;
        List<Path> paths;
        try {
            reader = new TsFileSequenceReader(tsFilePath);
            paths = reader.getAllPaths();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (Path path : paths) {
            for (Map.Entry<Pattern, MetaEntity> entry : patternMap.entrySet()) {
                if (entry.getKey().matcher(path.getFullPath()).find()) {
                    changedMap.put(path, entry.getValue());
                    changedTimeSeriesMap.putIfAbsent(path, entry.getValue());
                    break;
                }
            }
        }

    }

    /**
     * This function execute the rewrite tsfile
     *
     * @return Map<Path, MetaEntity> 已修改的path及对应元数据信息
     */
    public Map<Path, MetaEntity> execute(Map<Pattern, MetaEntity> patternMap, Map<Path, MetaEntity> changedTimeSeriesMap) throws IOException {

        init(patternMap, changedTimeSeriesMap);
        if (changedMap.isEmpty()) {
            FileUtils.copyFile(new File(tsFilePath), new File(targetTsFilePath));
            return changedMap;
        }

        try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFilePath);
             TsFileIOWriter writer = new TsFileIOWriter(new File(targetTsFilePath))) {
            // read devices
            TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
            while (deviceIterator.hasNext()) {

                Pair<String, Boolean> deviceInfo = deviceIterator.next();
                String device = deviceInfo.left;
                boolean aligned = deviceInfo.right;
                // write chunkGroup header
                writer.startChunkGroup(device);
                // write chunk & page data
                if (aligned) {
                    rewriteAlgined(reader, writer, device);
                } else {
                    rewriteNotAligned(device, reader, writer);
                }
                // chunkGroup end
                writer.endChunkGroup();

            }
            // write index,bloom,footer, end file
            writer.endFile();
        } catch (Exception e) {
            throw new IOException(e);
        }
        return changedMap;
    }

    private void rewriteAlgined(TsFileSequenceReader reader, TsFileIOWriter writer, String device)
            throws IOException {
        List<AlignedChunkMetadata> alignedChunkMetadatas = reader.getAlignedChunkMetadata(device);
        if (alignedChunkMetadatas == null || alignedChunkMetadatas.isEmpty()) {
            logger.warn("[alter timeseries] device({}) alignedChunkMetadatas is null", device);
            return;
        }
        boolean needRewrite = false;
        List<AlignedChunkMetadata> alignedChunkMetadataList = reader.getAlignedChunkMetadata(device);
        for (AlignedChunkMetadata metadata : alignedChunkMetadataList) {
            List<IChunkMetadata> valueChunkMetadataList = metadata.getValueChunkMetadataList();
            for (IChunkMetadata meta : valueChunkMetadataList) {
                String measurement = meta.getMeasurementUid();
                Path path = new Path(device, measurement);
                if (changedMap.containsKey(path)) {
                    needRewrite = true;
                    break;
                }
            }
            if (needRewrite) {
                break;
            }
        }
        Pair<Boolean, Pair<List<IMeasurementSchema>, List<IMeasurementSchema>>> schemaPair =
                collectSchemaList(alignedChunkMetadatas, reader, device);
        if (!needRewrite) {
            // fast: rewrite chunk data to tsfile
            alignedFastWrite(reader, writer, alignedChunkMetadatas);
        } else {
            // need to rewrite
            try {
                alignedRewritePoints(reader, writer, device, alignedChunkMetadatas, schemaPair);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void alignedRewritePoints(
            TsFileSequenceReader reader,
            TsFileIOWriter writer,
            String device,
            List<AlignedChunkMetadata> alignedChunkMetadatas,
            Pair<Boolean, Pair<List<IMeasurementSchema>, List<IMeasurementSchema>>> schemaPair)
            throws Exception {
        Pair<List<IMeasurementSchema>, List<IMeasurementSchema>> listPair = schemaPair.right;
        List<IMeasurementSchema> schemaList = listPair.left;
        List<IMeasurementSchema> schemaOldList = listPair.right;

        AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(schemaList);
        TsFileAlignedSeriesReaderIterator readerIterator =
                new TsFileAlignedSeriesReaderIterator(reader, alignedChunkMetadatas, schemaOldList);

        while (readerIterator.hasNext()) {
            Pair<AlignedChunkReader, Long> chunkReaderAndChunkSize = readerIterator.nextReader();
            AlignedChunkReader chunkReader = chunkReaderAndChunkSize.left;
            while (chunkReader.hasNextSatisfiedPage()) {
                IBatchDataIterator batchDataIterator = chunkReader.nextPageData().getBatchDataIterator();
                while (batchDataIterator.hasNext()) {
                    TsPrimitiveType[] prePointsData = (TsPrimitiveType[]) batchDataIterator.currentValue();
                    TsPrimitiveType[] pointsData = changedTSDataType(prePointsData, schemaList, schemaOldList);
                    long time = batchDataIterator.currentTime();
                    chunkWriter.write(time, pointsData);
                    batchDataIterator.next();
                }
            }
        }
        chunkWriter.writeToFileWriter(writer);
    }

    private TsPrimitiveType[] changedTSDataType(TsPrimitiveType[] pointsData, List<IMeasurementSchema> schemaList, List<IMeasurementSchema> oldSchemaList) throws Exception {
        for (int i = 0; i < schemaList.size(); i++) {
            if (schemaList.get(i).getType() != oldSchemaList.get(i).getType()) {
                pointsData[i] = changeTsDataTypeWithType(pointsData[i], schemaList.get(i).getType(), oldSchemaList.get(i).getType());
            }
        }
        return pointsData;
    }

    private TsPrimitiveType changeTsDataTypeWithType(TsPrimitiveType type, TSDataType dataType, TSDataType oldDataType) throws Exception {
        switch (oldDataType) {
            case INT32:
                return new TsPrimitiveType.TsLong(type.getInt());
            case FLOAT:
                return new TsPrimitiveType.TsDouble(type.getFloat());
            default:
                throw new Exception("changeTsDataTypeWithType error : type +" + type.toString() + ", dataType " + dataType.toString() + "oldDataType : " + oldDataType);
        }
    }

    private void alignedFastWrite(
            TsFileSequenceReader reader,
            TsFileIOWriter writer,
            List<AlignedChunkMetadata> alignedChunkMetadatas)
            throws IOException {
        for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadatas) {
            // write time chunk
            IChunkMetadata timeChunkMetadata = alignedChunkMetadata.getTimeChunkMetadata();
            Chunk timeChunk = reader.readMemChunk((ChunkMetadata) timeChunkMetadata);
            writer.writeChunk(timeChunk, (ChunkMetadata) timeChunkMetadata);
            // write value chunks
            List<IChunkMetadata> valueChunkMetadataList =
                    alignedChunkMetadata.getValueChunkMetadataList();
            for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
                if (chunkMetadata == null) {
                    continue;
                }
                Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
                writer.writeChunk(chunk, (ChunkMetadata) chunkMetadata);
            }
        }
    }

    protected Pair<Boolean, Pair<List<IMeasurementSchema>, List<IMeasurementSchema>>>
    collectSchemaList(
            List<AlignedChunkMetadata> alignedChunkMetadatas,
            TsFileSequenceReader reader,
            String device)
            throws IOException {
        List<IMeasurementSchema> schemaList = new ArrayList<>();
        List<IMeasurementSchema> schemaOldList = new ArrayList<>();
        Set<String> measurementSet = new HashSet<>();
        boolean allSame = true;
        for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadatas) {
            List<IChunkMetadata> valueChunkMetadataList =
                    alignedChunkMetadata.getValueChunkMetadataList();
            for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
                if (chunkMetadata == null) {
                    continue;
                }
                String measurementId = chunkMetadata.getMeasurementUid();
                if (measurementSet.contains(measurementId)) {
                    continue;
                }
                Path path = new Path(device, measurementId);
                measurementSet.add(measurementId);
                Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
                ChunkHeader header = chunk.getHeader();
                MeasurementSchema measurementSchema =
                        new MeasurementSchema(
                                header.getMeasurementID(),
                                header.getDataType(),
                                header.getEncodingType(),
                                header.getCompressionType());
                schemaOldList.add(measurementSchema);
                MetaEntity metaEntity = changedMap.get(path);
                if (metaEntity != null) {
                    measurementSchema =
                            new MeasurementSchema(
                                    header.getMeasurementID(),
                                    metaEntity.getDataType(),
                                    metaEntity.getTsEncoding(),
                                    metaEntity.getCompressionType());
                    schemaList.add(measurementSchema);
                } else {
                    schemaList.add(measurementSchema);
                }
            }
        }
        schemaList.sort(Comparator.comparing(IMeasurementSchema::getMeasurementId));
        schemaOldList.sort(Comparator.comparing(IMeasurementSchema::getMeasurementId));
        return new Pair<>(allSame, new Pair<>(schemaList, schemaOldList));
    }

    private void rewriteNotAligned(String device, TsFileSequenceReader reader, TsFileIOWriter writer)
            throws IOException {
        Map<String, List<ChunkMetadata>> measurementMap = reader.readChunkMetadataInDevice(device);
        if (measurementMap == null) {
            logger.warn("[alter timeseries] device({}) measurementMap is null", device);
            return;
        }
        for (Map.Entry<String, List<ChunkMetadata>> next : measurementMap.entrySet()) {
            String measurementId = next.getKey();
            List<ChunkMetadata> chunkMetadatas = next.getValue();
            if (chunkMetadatas == null || chunkMetadatas.isEmpty()) {
                logger.warn("[alter timeseries] empty measurement({})", measurementId);
                return;
            }
            // target chunk writer
            ChunkMetadata firstChunkMetadata = chunkMetadatas.get(0);
            ChunkWriterImpl chunkWriter = null;
            Path path = new Path(device, firstChunkMetadata.getMeasurementUid());
            for (ChunkMetadata chunkMetadata : chunkMetadatas) {
                // old mem chunk
                Chunk currentChunk = reader.readMemChunk(chunkMetadata);
                if (!changedMap.containsKey(path)) {
                    // fast write chunk
                    writer.writeChunk(currentChunk, chunkMetadata);
                    continue;
                }
                if (chunkWriter == null) {
                    // chunkWriter init
                    chunkWriter =
                            new ChunkWriterImpl(
                                    new MeasurementSchema(
                                            measurementId,
                                            changedMap.get(path).getDataType(),
                                            changedMap.get(path).getTsEncoding(),
                                            changedMap.get(path).getCompressionType()));
                }

                // generate new chunk
                IChunkReader chunkReader = new ChunkReader(currentChunk, null);
                while (chunkReader.hasNextSatisfiedPage()) {
                    IPointReader batchIterator = chunkReader.nextPageData().getBatchDataIterator();
                    while (batchIterator.hasNextTimeValuePair()) {
                        TimeValuePair timeValuePair = batchIterator.nextTimeValuePair();
                        writeTimeAndValueToChunkWriter(chunkWriter, timeValuePair, changedMap.get(path));
                    }
                }
                // flush
                chunkWriter.writeToFileWriter(writer);
            }
        }
    }

    /**
     * write time and value to chunkWriter
     */
    private void writeTimeAndValueToChunkWriter(
            ChunkWriterImpl chunkWriter, TimeValuePair timeValuePair, MetaEntity metaEntity) {
        switch (chunkWriter.getDataType()) {
            case TEXT:
                chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
                break;
            case FLOAT:
                chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
                break;
            case DOUBLE:
                if (timeValuePair.getValue().getDataType() == TSDataType.FLOAT && metaEntity.getDataType() == TSDataType.DOUBLE) {
                    chunkWriter.write(timeValuePair.getTimestamp(), (double) timeValuePair.getValue().getFloat());
                } else {
                    chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
                }
                break;
            case BOOLEAN:
                chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
                break;
            case INT64:
                if (timeValuePair.getValue().getDataType() == TSDataType.INT32 && metaEntity.getDataType() == TSDataType.INT64) {
                    chunkWriter.write(timeValuePair.getTimestamp(), (long) timeValuePair.getValue().getInt());
                } else {
                    chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
                }
                break;
            case INT32:
                chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
                break;
            case VECTOR:
                break;
            default:
                throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
        }
    }

}
