import java.util.Arrays;

/**
 * Created by ibf on 03/18.
 */
public class Test {
    public static void main(String[] args) {
        String str = "101:上海:25";
        String[] arr = str.split(":");
        System.out.println(arr.length);
        System.out.println(Arrays.toString(arr));
    }
}
