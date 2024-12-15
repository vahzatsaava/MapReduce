import java.io.File;
import java.util.*;

public class MapReduceSimulation {
    public static void main(String[] args) {
        clearFiles();
        List<String> inputFiles = Arrays.asList("file1.txt", "file2.txt", "file3.txt");
        int numReduceTasks = 3;
        int numWorkers = 4;

        Coordinator coordinator = new Coordinator(inputFiles, numReduceTasks);
        List<Thread> workers = new ArrayList<>();

        for (int i = 0; i < numWorkers; i++) {
            workers.add(new Thread(new Worker(coordinator)));
        }

        workers.forEach(Thread::start);
        workers.forEach(worker -> {
            try {
                worker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        System.out.println("MapReduce job completed.");
    }
    private static void clearFiles() {
        File folder = new File(".");
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.getName().startsWith("mr-") || file.getName().startsWith("final-")) {
                    file.delete();
                }
            }
        }
    }

}
