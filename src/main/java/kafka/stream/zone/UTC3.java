package kafka.stream.zone;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class UTC3 {
	
	public static final int TIME_ZONE = 2;
	public static final ZoneId TIMEZONE = ZoneId.of("GMT+" + TIME_ZONE);

	private UTC3() {}
	
	public static final DateTimeFormatter ZONE_FORMAT = DateTimeFormatter.ISO_ZONED_DATE_TIME;
	
	public static ZonedDateTime nowZoneDateTime() {
		return ZonedDateTime.now(TIMEZONE);
	}

	public static LocalDate nowDate() {
		return nowZoneDateTime().toLocalDate();
	}
	
	public static LocalDate tmrDate() {
		return nowZoneDateTime().toLocalDate().plusDays(1);
	}
	
	public static LocalDate yesterdayDate() {
		return nowZoneDateTime().toLocalDate().minusDays(1);
	}
	
	public static LocalDate twoDatesBackDate() {
		return nowZoneDateTime().toLocalDate().minusDays(2);
	}
	
	public static LocalDateTime nowDateTime() {
		return nowZoneDateTime().toLocalDateTime();
	}
	
	public static ZonedDateTime startOfToday(LocalDate date) {
		return ZonedDateTime.of(date.atTime(00,00,00), TIMEZONE);
	}
	
	public static ZonedDateTime endOfToday(LocalDate date) {
		return ZonedDateTime.of(date.atTime(23,59,59), TIMEZONE);
	}
	
	public static ZonedDateTime get(LocalDateTime dateTime) {
		return ZonedDateTime.of(dateTime, TIMEZONE);
	}
	
	public static ZonedDateTime parse(String input) {
		return ZonedDateTime.parse(input, ZONE_FORMAT);
	}

}
