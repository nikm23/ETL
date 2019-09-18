package etl;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Export_database {
	Connection con_old;
	PreparedStatement pst_old;
	public int Export(String db_name,String file_name) {
		// TODO Auto-generated constructor stub
		con_old=Connect_db.getConnection(db_name);
		try {
			System.out.println("exporting from "+db_name+" to "+file_name+"...");
			//delete file if exist
			File file=new File("C:\\Users\\himanshu\\Desktop\\"+file_name);
			if(file.exists()){
				if(file.delete()){
					System.out.println("old version of"+file.getName()+" is deleted");
				}
				else
				{
					System.out.println("problem in deleting "+file.getName());
					System.exit(0);
				}
			}
			
			String file_path="'C:\\\\Users\\\\himanshu\\\\Desktop\\\\"+file_name+"\'";
			//System.out.println(file_path);
			pst_old=con_old.prepareStatement("SELECT * FROM emp_name1 INTO OUTFILE "+file_path+" FIELDS TERMINATED BY \',\'ENCLOSED BY \'\"\'LINES TERMINATED BY \'\\r\\n\'");
			ResultSet rs_old=pst_old.executeQuery();
			rs_old.close();
			pst_old.close();
			return(1);//success
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;//not success
		}	
	}
	public static void main(String[] args){
	
	/*int i=	(new Export_database()).Export("db_old", "db_old.csv");
		System.out.println("i="+i);*/
	}
}
