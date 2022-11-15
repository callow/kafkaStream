package kafka.stream.zone;

public class Test {

	public static void main(String[] args) {
		
		// 现在时间 - 6小时
		System.out.println(UTC3.nowDate());
		System.out.println(UTC3.nowDateTime());
		System.out.println(UTC3.nowZoneDateTime());
		
	}
}
