package etl;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class Temp {
	Connection con_old;
	PreparedStatement pst_old;
	public int Export(String db_name,String file_name,int start,int end) {
		// TODO Auto-generated constructor stub
		con_old=Connect_db.getConnection(db_name);
		try {
			System.out.println("exporting from "+db_name+" to "+file_name+"...");
			//delete file if exist
			File file=new File("C:\\Users\\himanshu\\Desktop\\"+file_name);
			if(file.delete()){
				System.out.println("old version of"+file.getName()+" is deleted");
			}
			else
			{
				System.out.println("problem in deleting "+file.getName());
			}
			String file_path="'C:\\\\Users\\\\himanshu\\\\Desktop\\\\"+file_name+"\'";
			//System.out.println(file_path);
			//pst_old=con_old.prepareStatement("set @rownum=0;SELECT name,salary,phone,adress FROM (select (@rownum:=@rownum+1) as num,name,salary,phone,adress from emp_name) tbl where tbl.num>"+start+" and tbl.num<"+end+" INTO OUTFILE "+file_path+" FIELDS TERMINATED BY \',\'ENCLOSED BY \'\"\'LINES TERMINATED BY \'\\r\\n\'");
			pst_old=con_old.prepareStatement("SELECT name,salary,phone,adress FROM emp_name1 where row_number>"+start+" and row_number<"+end+" INTO OUTFILE "+file_path+" FIELDS TERMINATED BY \',\'ENCLOSED BY \'\"\'LINES TERMINATED BY \'\\r\\n\'");
			
			ResultSet rs_old=pst_old.executeQuery();
			ResultSet rs_old1=pst_old.getResultSet();
			int row_count=0;
		/*	while(rs_old1.next()){
				row_count++;
				if(row_count>0)
					break;
			}*/
			rs_old.close();
			rs_old1.close();
			pst_old.close();
			if(row_count==0)
				return 0;
			else
			return(1);//success
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;//not success
		}	
	}
	public static void main(String[] args){
	
	int i=	(new Temp()).Export("db_old", "db_old.csv",2,4);
		System.out.println("i="+i);
	}
}
