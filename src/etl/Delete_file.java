package etl;

import java.io.File;

public class Delete_file {
	public static void main(String[] args){
		//File file=new File("C:\\Users\\himanshu\\Desktop\\db_old.sql"+ "_part" + "2" + ".txt");
		File file=new File("C:\\Users\\himanshu\\Desktop\\db_old.csv");
		if(file.delete()){
			System.out.println(file.getName()+" is deleted");
		}
		else
		{
			System.out.println("problem in deleting "+file.getName());
		}
	}
}
