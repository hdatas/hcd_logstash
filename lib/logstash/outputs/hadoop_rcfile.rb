require "java"

java_import "org.apache.hadoop.hive.ql.io.RCFile"
java_import "org.apache.hadoop.conf.Configuration"
java_import "org.apache.hadoop.fs.FileSystem"
java_import "org.apache.hadoop.io.SequenceFile"
java_import "org.apache.hadoop.io.compress.CompressionCodec"
java_import "org.apache.hadoop.io.compress.GzipCodec"
java_import "org.apache.hadoop.io.compress.DefaultCodec"
java_import "org.apache.hadoop.fs.Path"
java_import "org.apache.hadoop.hive.conf.HiveConf"
java_import "org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable"
java_import "org.apache.hadoop.hive.serde2.columnar.BytesRefWritable"

class HadoopRCFile
  
  def initialize(file_path, column_num)
    @conf = HiveConf.new()
    @fs = FileSystem.getLocal(@conf)
    @path = Path.new(file_path)
    @conf.setInt(java.lang.String.new("hive.io.rcfile.column.number.conf"), column_num)
    @conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec")
    @compressor = DefaultCodec.new()
    @compressor.setConf(@conf)
    @writer = RCFile::Writer.new(@fs, @conf, @path, nil, @compressor)
#    @writer = RCFile::Writer.new(@fs, @conf, @path, 4096, 1, 32 * 1024 * 1024, nil, SequenceFile::Metadata.new(), nil)
    @cols = BytesRefArrayWritable.new(column_num)
  end

  public
  def write(columns)
    setup_cols(columns)
    @writer.append(@cols)
  end

  private
  def setup_cols(columns)
    i = 0
    columns.each do |column|
      if column
        col_str = java.lang.String.new(column)
      else
        col_str = java.lang.String.new("")
      end
      col = BytesRefWritable.new(col_str.getBytes())
      @cols.set(i, col)
      i = i + 1  
    end
  end

  public
  def close()
    @writer.close()
  end

end
