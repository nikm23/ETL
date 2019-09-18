package etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Vector;

import javax.swing.JOptionPane;


public class Test {
	String old_db="db_old";
	String new_db="db_new";
	Connection con_old,con_new;
	PreparedStatement pst_old,pst_new;
	String old_name,first_name,mid_name,last_name,old_phone,new_phone,old_adress,new_adress;
	int old_salary=0,new_salary=0;
	public void modify_name(String name){
		int first=name.trim().indexOf(" ");
		if(first!=-1)
		{
			first_name=mid_name=last_name="";
			//System.out.println("first="+first);
			first_name=name.substring(0,first);
			int i=first+1;
			while(name.charAt(i)==' ')
				i++;
			int next=name.trim().indexOf(" ",i);
			if(next!=-1){
				mid_name=name.trim().substring(i, next);
				i=next+1;
				while(name.charAt(i)==' ')
					i++;
				last_name=name.trim().substring(i);
			}
			else
			{
				mid_name=name.trim().substring(i);
			}
		}
		else{
			first_name=name.trim();
		}
	}
	public void modify_salary(int salary){
		new_salary=(int) (salary+(0.1*salary));
	}
	public void modify_phone(String phone){
		new_phone=String.valueOf("+91"+phone);
	}
	public void modify_adress(String adress){
		new_adress=adress.toUpperCase();
	}
	public void insert_one_by_one(){
		con_old=Connect_db.getConnection(old_db);
		con_new=Connect_db.getConnection(new_db);
		try {
			System.out.println("Extracting from "+old_db+" and inserting into "+new_db);
			pst_old=con_old.prepareStatement("select name,salary,phone,adress from emp_name1");
			ResultSet rs_old=pst_old.executeQuery();
			while (rs_old.next())
			   {
					//splitting name in old
					old_name="";
					old_salary=0;
					old_phone="";
					old_adress="";
					old_name=String.valueOf(rs_old.getString(1)).trim();
					old_salary=rs_old.getInt(2);
					old_phone=String.valueOf(rs_old.getString(3)).trim();
					old_adress=String.valueOf(rs_old.getString(4)).trim();
					modify_name(old_name);
					modify_salary(old_salary);
					modify_phone(old_phone);
					modify_adress(old_adress);
					//inserting name in new
					pst_new=con_new.prepareStatement("insert into emp_name(first_name,middle_name,last_name,salary,phone,adress) values(?,?,?,?,?,?)");
					pst_new.setString(1,first_name);
					pst_new.setString(2,mid_name);
					pst_new.setString(3,last_name);
					pst_new.setInt(4,new_salary);
					pst_new.setInt(5,new_salary);
					pst_new.setInt(6,new_salary);
					int x=pst_new.executeUpdate();
					pst_new.close();
			   }
			   rs_old.close();
			   pst_old.close();
			   System.out.println("successfully Extracted from "+old_db+" and inserting into "+new_db);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("error in Extracting from "+old_db+" and inserting into "+new_db);
		}	
	}
	public static void main(String[] args){
		long start = System.currentTimeMillis();
		Test obj=new Test();
		obj.insert_one_by_one();
		long end = System.currentTimeMillis();
        System.out.println("Taken time[sec]:");
        System.out.println((end - start) / 1000);
        
	}
}
