import localapp.LocalApp;
import manager.Manager;
import worker.Worker;

import java.util.Arrays;

public class SarcasmAnalysis {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: sarcasm-analysis [manager|worker] [args...]");
            return;
        }

        String[] emptyArgs = new String[0];

        switch (args[0].toLowerCase()) {
            case "manager":
                System.setProperty("logFileName", "manager");
                Manager.main(emptyArgs);
                break;
            case "worker":
                System.setProperty("logFileName", "worker");
                Worker.main(emptyArgs);
                break;
            default: // First input file path
                System.setProperty("logFileName", "localapp");
                LocalApp.main(args);
        }
    }
}
