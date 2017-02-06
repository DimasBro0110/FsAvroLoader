import java.io.File;
import java.io.IOException;

/**
 * Created by DmitriyBrosalin on 05/02/2017.
 */
public class FsLoaderMain {

    /*
    *
    * /Users/DmitriyBrosalin/FsAvroLoader/src/main/resources/test.avcs
    * /Users/DmitriyBrosalin/BigData/AvroFiles
    * 5
    * 5
    * */

    public static void main(String[] args) throws IOException {
        if(args.length < 0){
            System.exit(1);
        }else{
            String pathToAvroSchema = args[0];
            String pathToLoad = args[1];
            int amountRecordsInFile = Integer.parseInt(args[2]);
            int amountOfFiles = Integer.parseInt(args[3]);
            AvroCreater avroCreater =
                    new AvroCreater(new File(pathToAvroSchema), amountRecordsInFile, amountOfFiles);
            avroCreater.loadFiles(pathToLoad);
        }
    }

}
