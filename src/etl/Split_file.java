package etl;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class Split_file {

	public int create_file(String old_file_path,int lines_per_file){
		//first argument is the file path
        //File file = new File("C:\\Users\\himanshu\\Desktop\\db_old.sql");
		File file = new File(old_file_path);
		String extension=old_file_path.substring(old_file_path.indexOf("."), old_file_path.length());
        old_file_path=old_file_path.substring(0,old_file_path.indexOf("."));
		//System.out.println(extension);
		//second argument is the number of lines per chunk
        //In particular the smaller files will have numLinesPerChunk lines
        int numLinesPerChunk = lines_per_file;

        BufferedReader reader = null;
        BufferedWriter writer = null;
        try {
            reader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String line;        

        long start = System.currentTimeMillis();
        int i=0;
        try {
        	line=reader.readLine();
            for (i = 1; line != null; i++) {
                writer = new BufferedWriter(new FileWriter(old_file_path + "_part" + i + extension));
                for (int j = 0; j < numLinesPerChunk && line != null; j++) {
                    writer.write(line);
                    writer.newLine();
                    line = reader.readLine();
                }
                writer.flush();
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("Taken time[sec]:");
        System.out.println((end - start) / 1000);
        System.out.println("number of sub files created="+(i-1));
        return(i-1);
	}
    public static void main(String[] args) {
    	(new Split_file()).create_file("C:\\Users\\himanshu\\Desktop\\db_old.csv",4);
    }

}