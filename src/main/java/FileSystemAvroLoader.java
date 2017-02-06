import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * Created by DmitriyBrosalin on 05/02/2017.
 */
public class FileSystemAvroLoader {

    private int amountFiles;
    private String pathToLoad;
    private AvroCreater avroCreater;
    private static Random random = new Random();

    FileSystemAvroLoader(int amountFiles, String pathToLoad, AvroCreater avroCreater){
        this.pathToLoad = pathToLoad;
        this.amountFiles = amountFiles;
        this.avroCreater = avroCreater;
    }

    public void loadFiles() throws IOException {
        int counter = 0;
        DatumWriter<GenericRecord> writer =
                new GenericDatumWriter<GenericRecord>(this.avroCreater.getScema());
        DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(writer);
        while(counter != this.amountFiles) {
            File file = new File(this.pathToLoad + "test_load" + String.valueOf(random.nextLong()));
            fileWriter.create(this.avroCreater.getScema(), file);
            for(GenericRecord gen: this.avroCreater.createGenericRecord()){
                fileWriter.append(gen);
            }
        }
        fileWriter.close();
    }
}
