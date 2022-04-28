import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Message {
    private static final String TEXT_RESET = "\u001B[0m";
    // private static final String TEXT_BLACK = "\u001B[30m";
    private static final String TEXT_RED = "\u001B[31m";
    private static final String TEXT_GREEN = "\u001B[32m";
    private static final String TEXT_YELLOW = "\u001B[33m";
    private static final String TEXT_BLUE = "\u001B[34m";
    // private static final String TEXT_PURPLE = "\u001B[35m";
    // private static final String TEXT_CYAN = "\u001B[36m";
    // private static final String TEXT_WHITE = "\u001B[37m";

    public static void info(String _message, int _indent) {
        message("[INFO] " + _message, _indent);
    }

    public static void process(String _message, int _indent) {
        message(TEXT_BLUE + "[PROCESS] " + _message + " ... ", _indent);
    }

    public static void error(String _message, int _indent) {
        message(TEXT_YELLOW + "[ERROR] " + _message, _indent);
    }

    public static void success(String _message, int _indent) {
        message(TEXT_GREEN +  "[SUCCESS] " + _message, _indent);
    }

    public static void failed(String _message, int _indent) {
        message(TEXT_RED + "[FAILED] " + _message, _indent);
    }

    private static void message(String _message, int _indent) {
        System.out.print(LocalDateTime.now().format(DateTimeFormatter.ofPattern("H:m:s")) + "\t\t");
        System.out.print("<" + Thread.currentThread().getName() + ">\t\t");

        if (_indent > 0) {
            for (int i = 0; i < _indent; i++) System.out.print("\t");
            System.out.print("-> ");
        }

        System.out.println(_message + TEXT_RESET);
    }
}
