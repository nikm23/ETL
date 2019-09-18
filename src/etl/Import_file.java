package etl;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Import_file {
	Connection con_new;
	PreparedStatement pst_new;
	public int Import(String db_name,String file_name) {
		// TODO Auto-generated constructor stub
		con_new=Connect_db.getConnection(db_name);
		try {
			System.out.println("importing from "+file_name+" to "+db_name+"...");
			String file_path="'C:\\\\Users\\\\himanshu\\\\Desktop\\\\"+file_name+"\'";
			//System.out.println(file_path);
			pst_new=con_new.prepareStatement("LOAD DATA INFILE "+file_path+" INTO TABLE emp_name FIELDS TERMINATED BY \',\' ENCLOSED BY \'\"\'LINES TERMINATED BY \'\\r\\n\'IGNORE 1 ROWS;");
			pst_new.executeUpdate();
			pst_new.close();
			return(1);//success
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;//not success
		}	
	}
	public static void main(String[] args){
	
	/*int i=	(new Import_file()).Import("db_new_file", "db_new.csv");
		System.out.println("i="+i);*/
	}
}
