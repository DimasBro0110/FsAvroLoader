import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Created by DmitriyBrosalin on 05/02/2017.
 */
public class AvroCreater {

    private Schema schema;
    private int amountRecords;
    private static Random rnd = new Random();
    private int amountFiles;

    AvroCreater(File fileToAvroSchema, int amountRecords, int amountFiles){
        try{
            this.schema = new Schema.Parser().parse(fileToAvroSchema);
            this.amountRecords = amountRecords;
            this.amountFiles = amountFiles;
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public Schema getScema(){
        return this.schema;
    }

    public List<GenericRecord> createGenericRecord(){
        List<GenericRecord> lstGenericRecord = new LinkedList<GenericRecord>();
        int temp = this.amountRecords;
        while(temp != 0){
            GenericRecord testRecord = new GenericData.Record(this.schema);
            testRecord.put("msidn", rnd.nextLong());
            testRecord.put("lacCell", "lacCell test: " + String.valueOf(rnd.nextGaussian()));
            testRecord.put("mainUrl", "mainUrl test: " + String.valueOf(rnd.nextGaussian()));
            testRecord.put("cookie", "cookie test: " + String.valueOf(rnd.nextGaussian()));
            testRecord.put("protocol", "protocol test: " + String.valueOf(rnd.nextGaussian()));
            testRecord.put("timestampStart", rnd.nextLong());
            testRecord.put("timestampEnd", rnd.nextLong());
            testRecord.put("downloadKb", rnd.nextLong());
            testRecord.put("uploadKb", rnd.nextLong());
            testRecord.put("reason", "reason test: " + String.valueOf(rnd.nextGaussian()));
            lstGenericRecord.add(testRecord);
            temp -= 1;
        }
        return lstGenericRecord;
    }

    public byte[] marshalGenericRecord(GenericRecord genericRecord){
        try{
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(this.schema);
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
            writer.write(genericRecord, encoder);
            encoder.flush();
            return byteArrayOutputStream.toByteArray();
        }catch(Exception ex){
            ex.printStackTrace();
            return null;
        }
    }

    public GenericRecord demarshalGenricRecord(byte[] bytes){
        try{
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(this.schema);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);
            return reader.read(null, decoder);
        }catch (Exception ex){
            ex.printStackTrace();
            return null;
        }
    }

    public void loadFiles(String pathToLoad) throws IOException {
        int counter = 0;
        while(counter != this.amountFiles) {
            DatumWriter<GenericRecord> writer =
                    new GenericDatumWriter<GenericRecord>(this.schema);
            DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(writer);
            File file = new File(pathToLoad + "test_load" + String.valueOf(rnd.nextLong()) + ".avro");
            fileWriter.create(this.schema, file);
            for(GenericRecord gen: createGenericRecord()){
                fileWriter.append(gen);
            }
            fileWriter.close();
            counter += 1;
        }
    }

}
