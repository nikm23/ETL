package etl;

public class string {
	public static void main(String[] args){
		String file_path="C:\\Users\\himanshu\\Desktop\\db_old" + "_part" + "1" + ".csv";
		String part=file_path.substring(file_path.indexOf("part")+4,file_path.indexOf("part")+5);
		System.out.println("part:"+part);
	}
}
