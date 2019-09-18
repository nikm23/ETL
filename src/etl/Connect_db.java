package etl;
import java.sql.Connection;
import java.sql.DriverManager;

import javax.swing.JOptionPane;

public class Connect_db 
{
 public static Connection getConnection(String db_name)
	{
		Connection con=null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			con=DriverManager.getConnection("jdbc:mysql://localhost:3306/"+db_name,"root","");
			//JOptionPane.showMessageDialog(null, "connected...");
		   }
		catch (Exception e) 
		{
			System.out.println("error while establishing connection with:"+db_name);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("connection with "+db_name+" is successfully made");
		return con;
	}
 	public static void main(String []arg)
 	{
 		getConnection("db_old");
 	}
}
