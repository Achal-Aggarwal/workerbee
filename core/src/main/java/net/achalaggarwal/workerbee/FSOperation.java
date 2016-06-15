package net.achalaggarwal.workerbee;

import lombok.Getter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FSOperation {
  public static final boolean OVERWRITE = true;
  public static final boolean NOT_RECURSIVE = false;
  private final Configuration conf;

  @Getter
  private final String tempPath;

  public final String getAvroSchemaBasePath(){
    return getTempPath() + "/avro-schema";
  }

  public FSOperation(Configuration conf) {
    this.conf = conf;
    this.tempPath = conf.get("hadoop.tmp.dir") + "/workerbee/" + Utils.getRandomPositiveNumber();
  }

  public String createTempDir(){
    return getTempPath() + "/dir-" + Utils.getRandomPositiveNumber();
  }

  public String createTempFile(){
    return getTempPath() + "/file-" + Utils.getRandomPositiveNumber();
  }

  public <T extends TextTable> Path writeTextRow(T table, Row... rows) throws IOException {
    List<String> records = new ArrayList<>(rows.length);
    for (Row row : rows) {
      records.add(row.generateRecord());
    }

    return writeString(createTempFile(), Utils.joinList(records, System.lineSeparator()));
  }

  public <T extends AvroTable> Path writeAvroRow(T table, Row... rows) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);

    Path hdfsPath = new Path(createTempFile());
    fileSystem.mkdirs(hdfsPath.getParent());

    try(DataFileWriter<SpecificRecord> writer =
          new DataFileWriter<>(new SpecificDatumWriter<SpecificRecord>(table.getSchema()))){
      writer.create(table.getSchema(), fileSystem.create(hdfsPath, OVERWRITE));

      for (Row row : rows) {
        writer.append(RowUtils.getSpecificRecord(row));
      }
    }
    return hdfsPath;
  }

  public Path writeTableSchema(AvroTable table) throws IOException {
    return writeString(
      table.getSchemaPath(getAvroSchemaBasePath()),
      table.getSchema().toString(true)
    );
  }

  public List<String> readRecords(String path) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);
    Path hdfsPath = new Path(path);

    List<String> rows = new ArrayList<>();

    RemoteIterator<LocatedFileStatus> iter = fileSystem.listFiles(hdfsPath, NOT_RECURSIVE);
    while (iter.hasNext()) {
      rows.addAll(readFile(iter.next().getPath()));
    }

    return rows;
  }

  private List<String> readFile(Path hdfsPath) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);
    List<String> lines = new ArrayList<>();

    String line;
    try(BufferedReader dataInputStream = new BufferedReader(new InputStreamReader(fileSystem.open(hdfsPath)))){
      while ((line = dataInputStream.readLine())!= null){
        lines.add(line);
      }
    }

    return lines;
  }

  public Path writeString(String path, String content) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);

    Path hdfsPath = new Path(path);
    fileSystem.mkdirs(hdfsPath.getParent());

    try(FSDataOutputStream dataOutputStream = fileSystem.create(hdfsPath, OVERWRITE)){
      byte[] bytes = content.getBytes();
      dataOutputStream.write(bytes, 0, bytes.length);
    }

    return hdfsPath;
  }
}
