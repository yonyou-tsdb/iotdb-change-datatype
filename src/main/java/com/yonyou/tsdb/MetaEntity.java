package com.yonyou.tsdb;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class MetaEntity {

    private TSDataType dataType;

    private TSEncoding tsEncoding;

    private CompressionType compressionType;

    public MetaEntity(TSDataType dataType, TSEncoding tsEncoding, CompressionType compressionType) {
        this.dataType = dataType;
        this.tsEncoding = tsEncoding;
        this.compressionType = compressionType;
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public void setDataType(TSDataType dataType) {
        this.dataType = dataType;
    }

    public TSEncoding getTsEncoding() {
        return tsEncoding;
    }

    public void setTsEncoding(TSEncoding tsEncoding) {
        this.tsEncoding = tsEncoding;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }
}
