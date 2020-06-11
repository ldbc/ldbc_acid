package utils;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Utils {

    public static String readFile(String fileName) throws IOException {
        List<String> lines = Files.readLines(new File(fileName), StandardCharsets.UTF_8);
        return String.join(System.lineSeparator(), lines);
    }

}
