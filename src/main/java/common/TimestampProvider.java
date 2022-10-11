package common;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimestampProvider {
    static public String currentTimestamp() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss xxx");
        ZonedDateTime now = ZonedDateTime.now();

        return dtf.format(now);
    }
}
