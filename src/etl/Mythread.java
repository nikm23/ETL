package etl;

import java.util.concurrent.ConcurrentLinkedQueue;
class Execute implements Runnable {
	int arr[][],arr_copy[][];
	int count=0;
	private boolean cancel = false;
   private ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
   Execute(int a[][],int b[][]){
	   arr=a;
	   arr_copy=b;
   }
   public void run() {
       while(!cancel) {
    	   String data = queue.poll();
           if(data != null) {
        	   count++;
               int m=Integer.parseInt(data);
               arr_copy[m]=new int[arr[m].length];
               for(int i=0;i<arr[m].length;i++)
            	   arr_copy[m][i]=arr[m][i];
               if(count==5)
            	   cancel();
           } else 
           {
        	   try {
				(new Thread()).sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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


class FetchOperands implements Runnable {
	int arr[][];
	int sum=0;
	int count=0;
	private boolean cancel = false;
   private ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
   private Execute nextComponent;
   FetchOperands(int a[][]){
	   arr=a;
   }
   public void run() {
       while(!cancel) {
    	   String data = queue.poll();
           if(data != null) {
               count++;
        	   int m=Integer.parseInt(data);
               for(int i=0;i<arr[m].length;i++)
            	   sum+=arr[m][i];
               nextComponent.enqueue(data);
               if(count==5)
            	   cancel();
           }else 
           {
        	   try {
				(new Thread()).sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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

   public void setExecute(Execute nextComponent) {
       this.nextComponent = nextComponent;
   }
}

class Decode implements Runnable {
	int arr[][];
	int count=0;
		private boolean cancel = false;
	   private ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
	   private FetchOperands nextComponent;
	   Decode(int a[][]){
		   arr=a;
	   }
	   public void run() {
	       while(!cancel) {
	           String data = queue.poll();
	           if(data != null) {
	        	  count++;
	        	   int i=Integer.parseInt(data);
	        	   arr[i]=new int[i+2];
	        	   for(int j=0;j<i+2;j++)
	        		   arr[i][j]=j;
	               nextComponent.enqueue(data);
	               if(count==5)
	            	   cancel();
	           } else 
	           {
	        	   try {
					(new Thread()).sleep(500);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
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

	   public void setFetchOperands(FetchOperands nextComponent) {
	       this.nextComponent = nextComponent;
	   }
}
public class Mythread extends Thread
{
public static void main(String[] args)
{
	int arr[][]=new int[5][],copy_arr[][]=new int[5][];
	Decode obj_decode=new Decode(arr);
	Thread t_decode=new Thread(obj_decode);
	t_decode.setName("Decode Thread");
	FetchOperands obj_fetch=new FetchOperands(arr);
	Thread t_fetch=new Thread(obj_fetch);
	t_fetch.setName("fetch Thread");
	Execute obj_execute=new Execute(arr,copy_arr);
	Thread t_execute=new Thread(obj_execute);
	t_execute.setName("execute Thread");
	obj_decode.setFetchOperands(obj_fetch);
	obj_fetch.setExecute(obj_execute);
/****************************************************/
	for(int i=0;i<5;i++)
	{
		obj_decode.enqueue(String.valueOf(i));
	}
	t_decode.start();
	t_fetch.start();
	t_execute.start();
	//obj_decode.cancel();
	//obj_fetch.cancel();
	//obj_execute.cancel();
	try {
		t_decode.join();
		t_fetch.join();
		t_execute.join();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	System.out.println("finished");
	System.out.println("arr:");
	for(int i=0;i<arr.length;i++)
	{
		for(int j=0;j<arr[i].length;j++)
			System.out.printf(arr[i][j]+" ");
		System.out.println();
	}
	System.out.println("copy_arr:");
	for(int i=0;i<copy_arr.length;i++)
	{
		for(int j=0;j<copy_arr[i].length;j++)
			System.out.printf(copy_arr[i][j]+" ");
		System.out.println();
	}
	System.out.println("sum="+obj_fetch.sum);
}
}
