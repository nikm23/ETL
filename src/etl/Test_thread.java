package etl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.MaximizeAction;

import org.omg.CORBA.OBJ_ADAPTER;

class Load implements Runnable {
	private boolean cancel = false;
	String new_db;
	Load(String db){
		new_db=db;
	}
	String imported_file_name;
   private ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
   public void run() {	
		
		while(!cancel) {
	           String part = queue.poll();
	           if(part != null) {
	        	   if(!part.equals("end")){
	        		   int part_number=Integer.parseInt(part);
	        		   int j=	(new Import_file()).Import(new_db, imported_file_name+String.valueOf(part_number)+".csv");
			    		
			    		if(j!=1){
			    			System.out.println("error in importing from "+imported_file_name+String.valueOf(part_number)+".csv"+" to "+new_db);
			    		}
			    		else{
			    			System.out.println("successfully imported from "+imported_file_name+String.valueOf(part_number)+".csv"+" to "+new_db);
			    			if(new File("C:\\Users\\himanshu\\Desktop\\"+imported_file_name+String.valueOf(part_number)+".csv").delete()){
			    				System.out.println("new  subfile "+part_number+" deleted");
			    			}
			    			else
			    			{
			    				System.out.println("error in deleting new subfile "+part_number);
			    			}
			    		}
	        		   	        		   
	        	   }
	        	   else
	        	   {
	        		   cancel();
	        	   }
	        	  
	           }
	       }
	   
	
   }

   public void enqueue(String data) {
       queue.offer(data);
   }

   public void cancel() {
       cancel = true;
   }


}


class Transform implements Runnable {
	int lines_per_file;
	String imported_file_name;
	private boolean cancel = false;
   private ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
   private Load nextComponent;
   String old_name,first_name,mid_name,last_name,old_adress,new_adress,old_phone,new_phone;
   int old_salary,new_salary;
   public Transform(String imported_name) {
	// TODO Auto-generated constructor stub
	   imported_file_name=imported_name;
}
     public void run() {
	   try{
		   while(!cancel) {
	           String part = queue.poll();
	           if(part != null) {
	        	   if(!part.equals("end")){
	        		   int part_number=Integer.parseInt(part);
	        		   String exported_file_path="C:\\Users\\himanshu\\Desktop\\"+"db_old"+part_number+".csv";
	        		   BufferedReader br = new BufferedReader(new FileReader(exported_file_path));
			        	BufferedWriter wr=new BufferedWriter(new FileWriter("C:\\Users\\himanshu\\Desktop\\"+imported_file_name+String.valueOf(part_number)+".csv"));
			        	//initializing attributes
			        	String line;
			        	while ((line = br.readLine()) != null) {
			            		//System.out.println(line);
			            		old_name="";
			            		old_salary=0;
			            		old_phone="";
			            		old_adress="";
			            		String attributes[]=line.split(",");
			            		if(attributes.length!=4)
			            		{
			            			System.out.println("attribute length< 4 and ="+attributes.length);
			            		}
			            		else
			            		{
			            			//System.out.println(attributes.length);
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
			            		
			            }
			        	wr.flush();
			        	wr.close();
			        	br.close();
			        	System.out.println("subfile "+part_number+" successfully transformed");
			           //deleting subfile
			        	if(new File(exported_file_path).delete()){
			        		System.out.println("old subfile "+part_number+"deleted");
			        	}
			        	else
			        	{
			        		System.out.println("problem in deleting old subfile "+part_number);
						       
			        	}
			        	nextComponent.enqueue(String.valueOf(part_number));
	        	   }
	        	   else
	        	   {
	        		   nextComponent.enqueue("end");
	        		   
	        		   cancel();
	        	   }
	           } 
	       }
	   }
	   catch(Exception ex){
		   ex.printStackTrace();
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

      public void enqueue(String data) {
       queue.offer(data);
   }

   public void cancel() {
       cancel = true;
   }

   public void setLoad(Load nextComponent) {
       this.nextComponent = nextComponent;
       nextComponent.imported_file_name=imported_file_name;
         }

}

class Extract implements Runnable {
	
	int lines_per_file;
	String old_db;
	String exported_file_path;
		private boolean cancel = false;
	   private Transform nextComponent;
	   public Extract(int number_of_lines_per_file,String old_db,String exported){
			  lines_per_file=number_of_lines_per_file;
			  this.old_db=old_db;
			  exported_file_path=exported;
	   }
	   public void run() {   	
		   
		   try{

			   int part_number=0;
			  String original_exported_file_path=exported_file_path;
			   BufferedReader br=new BufferedReader(new FileReader(original_exported_file_path));
			   BufferedWriter wr=null;
			   String line;
			   long count=0;
			   while((line=br.readLine())!=null){
				   if((count%lines_per_file)==0){

					   
					   if(part_number>0){
						   System.out.println("old subfile "+(part_number-1)+" successfully made");
						   nextComponent.enqueue(String.valueOf(part_number-1));
						   
					   }
					   wr=new BufferedWriter(new FileWriter("C:\\Users\\himanshu\\Desktop\\"+"db_old"+part_number+".csv"));
					   part_number++;
				   }
				   count++;
				   wr.write(line);
				   wr.newLine();
			   }
			   System.out.println("old subfile "+(part_number-1)+" successfully made");
			   nextComponent.enqueue(String.valueOf(part_number-1));
			   nextComponent.enqueue("end");
			   wr.flush();wr.close();
		   }
		   catch(Exception ex){
			   ex.printStackTrace();
		   }
	   }
		    	    
	   public void setTransform(Transform nextComponent) {
	       this.nextComponent = nextComponent;
	       nextComponent.lines_per_file=lines_per_file;
	   }
	   
}

public class Test_thread extends Thread
{

public static void main(String[] args)
{
	long start = System.currentTimeMillis();
	
	String exported_file="db_old.csv";
	String imported_file="db_new";
	String old_db="db_old";
	String new_db="db_new_thread";
	int number_of_lines_per_file=150000;
	System.out.println("line="+number_of_lines_per_file);
	int max_number_of_files=1000000;
	//exprting database
	int i=(new Export_database()).Export(old_db, exported_file);
	if(i==1){
		System.out.println("successfully exported from "+old_db+" to "+exported_file);
		Extract obj_extract=new Extract(number_of_lines_per_file,old_db,"C:\\Users\\himanshu\\Desktop\\"+exported_file);
		Thread t_extract=new Thread(obj_extract);
		t_extract.setName("Extract Thread");
		Transform obj_transform=new Transform(imported_file);
		Thread t_transform=new Thread(obj_transform);
		t_transform.setName("transform Thread");
		Load obj_load=new Load(new_db);
		Thread t_load=new Thread(obj_load);
		t_load.setName("Load Thread");
		obj_extract.setTransform(obj_transform);
		obj_transform.setLoad(obj_load);
	/****************************************************/
		System.out.println("starting threads");
		t_extract.start();
		t_transform.start();
		t_load.start();
		try {
			t_extract.join();
			t_transform.join();
			t_load.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
	    System.out.println("Taken time[sec]:");
	    System.out.println((end - start) / 1000);
		

	}
	else{
		System.out.println("error in ecportin from "+old_db+" to "+exported_file);
	}
	}
}
