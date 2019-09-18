package etl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Read_file {
	public static void main(String[] args){
		
		try {
			BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\himanshu\\Desktop\\db_old.sql"));
			String line,name;
			try {
				while((line=br.readLine())!=null){
					if(!line.isEmpty() && line.charAt(0)=='I')
						break;
				}
				do{
					name=line.substring(line.indexOf("'")+1, line.indexOf("'",line.indexOf("'")+1));
					System.out.println(name);
				}while((line=br.readLine())!=null);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}
