public class MapTask {
    final int id;
    final String fileName;
    final int numReduceTasks;

    MapTask(int id, String fileName, int numReduceTasks) {
        this.id = id;
        this.fileName = fileName;
        this.numReduceTasks = numReduceTasks;
    }
}
