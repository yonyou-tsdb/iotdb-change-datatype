# iotdb-change-datatype
iotdb change datatype tool by rewrite tsfile and append mlog.bin

# 使用场景
     
     开始使用时数据类型一开始并不能完全确定，尤其是数据类型
     在真实数据满足目标数据类型的前提下，数据类型转换允许关系，目前支持修改数据类型
     int32 -> int64
     float -> double
     同时支持相同时间类型修改编码方式，压缩方式
     使用前提：需要停服
     
# 编译打包
   
     mvn clean compile assembly:single 
     
# 步骤1：修改前准备
  （1）应用层停止写入 
   
  （2）执行flush操作，关闭iotdb server
   
     cli命令行执行flush
     执行./stop-server.sh
    
  （3）备份待修改存储组的tsfile文件及mlog.bin文件
    
    
#    步骤2：修改tsfile和mlog.bin
（1）执行命令如下：
    对非对齐时间序列对应存储组执行
    
    java -jar change-datatype-1.0-SNAPSHOT-jar-with-dependencies.jar /xxx/xxx/data/sequence/root.iot/0/0 /xxx/xxx/xxx  /xxx/xxx/xxx/config.txt
    参数说明：
    tsfile所在文件夹：/xxx/xxx/data/sequence/root.iot/0/0
    iotdb服务system文件夹下schema下的mlog.bin path：/xxx/xxx/xxx
    修改时间序列配置：/xxx/xxx/xxx/config.txt
    新tsfile产生路径，会将路径中的data目录替换成dataTmp目录
     
 (2) 修改时间序列配置说明
  
    root.iot.*.东区15号模块单晶炉冷却水总管回水温度实际值 4 0 1
    root.iot.*.东区冷冻机组1号循环冷却水泵电流高报警 4 0 1
    root.iot.*.东区14号模块单晶炉冷却水总管回水温度实际值 4 0 1

    四列由空格分隔的配置
    第一列：需要修改的时间序列，通配符
    第二列：数据类型  boolean：0，int32：1，int64：2，float：3，double：4
    第三列：编码类型  plain：0，dictionary：1，rle：2，diff：3，ts_2diff：4，bitmap：5，regular：7，gorilla：8
    第四列：压缩类型  uncompressed：0，snappy：1，gzip：2，lz0：3，sdt：4，lz4：7
    数据类型，编码类型，压缩类型必须服务iotdb规范  
    
    
 # 步骤3：启动服务
（1）将新生成的tsfile及对应的mlog.bin文件替换掉，启动服务

    ./start-server.sh

 # 步骤4：验证
（1）验证对应时间序列是否已成功修改，数据是否正确

    show timeseries xxx.xxx.xxx验证schema是否修改
    select xxx from xxx.xxx.xxx抽查验证数据是否正确 
