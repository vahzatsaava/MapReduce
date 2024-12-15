import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Worker implements Runnable {
    private final Coordinator coordinator;

    public Worker(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void run() {
        while (true) {
            Object task = coordinator.getTask();
            if (task == null) {
                if (coordinator.allTasksCompleted()) {
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }

            if (task instanceof MapTask) {
                handleMapTask((MapTask) task);
            } else if (task instanceof ReduceTask) {
                handleReduceTask((ReduceTask) task);
            }
        }
    }

    private void handleMapTask(MapTask mapTask) {
        String content = readFile(mapTask.fileName);
        List<KeyValue> keyValues = map(content);

        Map<Integer, List<String>> fileMap = new HashMap<>();
        for (KeyValue kv : keyValues) {
            int bucket = (kv.key.hashCode() & Integer.MAX_VALUE) % mapTask.numReduceTasks;
            String fileName = "mr-" + mapTask.id + "-" + bucket;
            writeToFile(fileName, kv.key + " " + kv.value);
            fileMap.computeIfAbsent(bucket, k -> new ArrayList<>()).add(fileName);
        }

        coordinator.completeMapTask(fileMap);
    }

    private void handleReduceTask(ReduceTask reduceTask) {
        Map<String, List<String>> groupedData = new HashMap<>();
        for (String file : reduceTask.files) {
            List<KeyValue> keyValues = readKeyValuePairs(file);
            for (KeyValue kv : keyValues) {
                groupedData.computeIfAbsent(kv.key, k -> new ArrayList<>()).add(kv.value);
            }
        }

        List<String> results = new ArrayList<>();
        for (var entry : groupedData.entrySet()) {
            String result = reduce(entry.getValue());
            results.add(entry.getKey() + " " + result);
        }
        results.sort(String::compareTo);
        writeToFile("final-" + reduceTask.id + ".txt", results);

        coordinator.completeReduceTask();
    }

    private String readFile(String fileName) {
        try {
            return new String(Files.readAllBytes(Paths.get(fileName)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<KeyValue> readKeyValuePairs(String fileName) {
        List<KeyValue> kvList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(" ", 2);
                kvList.add(new KeyValue(parts[0], parts[1]));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return kvList;
    }

    private void writeToFile(String fileName, String content) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName, true))) {
            bw.write(content);
            bw.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeToFile(String fileName, List<String> contents) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName))) {
            for (String line : contents) {
                bw.write(line);
                bw.newLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<KeyValue> map(String content) {
        List<KeyValue> kvList = new ArrayList<>();
        String[] words = content.split("\\W+");
        for (String word : words) {
            if (!word.isEmpty()) {
                kvList.add(new KeyValue(word.toLowerCase(), "1"));
            }
        }
        return kvList;
    }

    private String reduce(List<String> values) {
        return String.valueOf(values.size());
    }
}
