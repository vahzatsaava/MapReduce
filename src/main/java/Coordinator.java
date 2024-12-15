import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Coordinator {
    private final Queue<MapTask> mapTasks = new ConcurrentLinkedQueue<>();
    private final Queue<ReduceTask> reduceTasks = new ConcurrentLinkedQueue<>();
    private final Map<Integer, List<String>> intermediateFiles = new ConcurrentHashMap<>();
    private final int totalMapTasks;
    private final int totalReduceTasks;
    private int completedMapTasks = 0;
    private int completedReduceTasks = 0;

    public Coordinator(List<String> inputFiles, int numReduceTasks) {
        this.totalMapTasks = inputFiles.size();
        this.totalReduceTasks = numReduceTasks;
        for (int i = 0; i < inputFiles.size(); i++) {
            mapTasks.add(new MapTask(i, inputFiles.get(i), numReduceTasks));
        }
    }

    public synchronized Object getTask() {
        if (!mapTasks.isEmpty()) {
            return mapTasks.poll();
        }
        if (completedMapTasks == totalMapTasks && !reduceTasks.isEmpty()) {
            return reduceTasks.poll();
        }
        return null;
    }

    public synchronized void completeMapTask(Map<Integer, List<String>> generatedFiles) {
        for (var entry : generatedFiles.entrySet()) {
            intermediateFiles.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).addAll(entry.getValue());
        }
        completedMapTasks++;

        if (completedMapTasks == totalMapTasks) {
            for (int i = 0; i < totalReduceTasks; i++) {
                reduceTasks.add(new ReduceTask(i, intermediateFiles.getOrDefault(i, Collections.emptyList())));
            }
        }
    }

    public synchronized void completeReduceTask() {
        completedReduceTasks++;
    }

    public synchronized boolean allTasksCompleted() {
        return completedReduceTasks == totalReduceTasks;
    }
}
