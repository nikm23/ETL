package etl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Vector;

import javax.swing.JOptionPane;


public class Test_file {
	String old_name,first_name,mid_name,last_name,old_phone,old_adress,new_phone,new_adress;
	int new_salary=0,old_salary=0;
	Test_file(){
			String line="";
			try {
				String exported_file="db_old.csv";
				String imported_file="db_new.csv";
				String old_db="db_old";
				String new_db="db_new_file";
				int i=(new Export_database()).Export(old_db, exported_file);
				if(i==1)
				{
					System.out.println("successfully exported from "+old_db+" to "+exported_file);
					BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\himanshu\\Desktop\\"+exported_file));
		        	BufferedWriter wr=new BufferedWriter(new FileWriter("C:\\Users\\himanshu\\Desktop\\"+imported_file));
		        	//initializing attributes
		        	
		        	wr.write("first_name,middle_name,last_name,salary,phone,adress");
		        	wr.newLine();
		        	while ((line = br.readLine()) != null) {
		            		//System.out.println(line);
		            		old_name="";
		            		old_salary=0;
		            		old_phone="";
		            		old_adress="";
		            		String attributes[]=line.split(",");
		            		old_name=attributes[0].substring(1, attributes[0].length()-1);
							old_salary=Integer.parseInt(attributes[1].substring(1, attributes[1].length()-1));
							old_phone=attributes[2].substring(1, attributes[2].length()-1);
							old_adress=attributes[3].substring(1, attributes[3].length()-1);
							modify_name(old_name);
							modify_salary(old_salary);
							modify_phone(old_phone);
							modify_adress(old_adress);
		            		//System.out.println("old_name="+old_name+"  first_name="+first_name+"  old_salary="+old_salary+"  new_salary="+new_salary);
							//inserting name in new
							wr.write("\""+first_name+"\",\""+mid_name+"\",\""+last_name+"\","+new_salary+",\""+new_phone+"\",\""+new_adress+"\"");
		            		wr.newLine();
		            }
		        	wr.flush();
		        	wr.close();
		        	br.close();
		        	int j=	(new Import_file()).Import(new_db, imported_file);
		    		
		    		if(j!=1){
		    			System.out.println("error in importing from "+imported_file+" to "+new_db);
		    		}
		    		else{
		    			System.out.println("successfully imported from "+imported_file+" to "+new_db);
		    			if(new File("C:\\Users\\himanshu\\Desktop\\"+exported_file).delete()){
		    				System.out.println("old  subfile "+exported_file+" deleted");
		    			}
		    			else
		    			{
		    				System.out.println("error in deleting old file "+exported_file);
		    			}
		    			if(new File("C:\\Users\\himanshu\\Desktop\\"+imported_file).delete()){
		    				System.out.println("new  subfile "+imported_file+" deleted");
		    			}
		    			else
		    			{
		    				System.out.println("error in deleting new file "+imported_file);
		    			}
		    		}
				}
				else
				{
					System.out.println("error in exporting from "+old_db+" to "+exported_file);
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			
	}
	public void modify_phone(String phone){
		new_phone=String.valueOf("+91"+phone);
	}
	public void modify_adress(String adress){
		new_adress=adress.toUpperCase();
	}
	public void modify_name(String name){
		first_name=mid_name=last_name="";
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
	public static void main(String[] args){
		long start = System.currentTimeMillis();
		new Test_file();
		long end = System.currentTimeMillis();
        System.out.println("Taken time[sec]:");
        System.out.println((end - start) / 1000);
	}
}
