package cn.cloud;

import org.junit.Test;

/**
 * Description:
 *
 * @author: Cloud
 * Date: 2019-02-19
 * Time: 13:47
 */
public class SeTest {
    @Test
    public void testSplit() {
        String line = "1363157985066 \t13726230503\t00-FD-07-A4-72-B8:CMCC\t120.196.100.82\ti02.c.aliimg.com\t\t24\t27\t2481\t24681\t200";
        String[] fields = line.split("\t");
        if(fields.length < 4) {
            return;
        }
        String phone = fields[1];
        long upflow = Long.parseLong(fields[fields.length - 3]);
        long downflow = Long.parseLong(fields[fields.length - 2]);
        System.out.println(phone);
    }
}
