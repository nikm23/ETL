package etl;

public class Sum extends Thread {
	int start_index,end_index,arr[],sum=0;
	Sum(int a[],int s,int e,String name){
		arr=a;
		start_index=s;
		end_index=e;
		setName(name);
	}
	public void run(){
		for(int i=start_index;i<=end_index;i++){
			System.out.println("Thread :"+getName()+" running");
			sum+=arr[i];
		}
		System.out.println("***Thread :"+getName()+" finished**");
	}
	public int get_sum()
	{
		return sum;
	}
	public static long thread_method(int arr[],int numbers_in_each_thread){
		int size=arr.length/numbers_in_each_thread;
		if(arr.length%numbers_in_each_thread!=0)
			size++;
		long  sum=0;
		Sum thrd[]=new Sum[size];
		for(int i=0;i<size;i++){
			if(numbers_in_each_thread*i+numbers_in_each_thread-1<arr.length)
			thrd[i]=new Sum(arr,numbers_in_each_thread*i,numbers_in_each_thread*i+numbers_in_each_thread-1,String.valueOf(i));
			else
				thrd[i]=new Sum(arr,numbers_in_each_thread*i,arr.length-1,String.valueOf(i));
			thrd[i].start();
		}
		for(int i=0;i<size;i++)
			try {
				while(thrd[i].isAlive()){};
				//thrd[i].join();
				sum+=thrd[i].get_sum();
				System.out.println("sum["+i+"]calculated");
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		System.out.println("sum="+sum);
		return sum;
	}
	public static void main(String[] args){
		final int arr_size=10000;
		int numbers_in_each_thread=500;
		int arr[]=new int[arr_size];
		for(int i=0;i<arr_size;i++)
			arr[i]=i;
		System.out.println("calculated by thread method sum="+thread_method(arr, numbers_in_each_thread));
		/*long sum=0;
		for(int i=0;i<arr_size;i++){
			sum+=arr[i];
		}
		System.out.println("calculated simply sum="+sum);*/
	}
}
