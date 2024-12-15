import java.util.List;

public class ReduceTask {
    final int id;
    final List<String> files;

    ReduceTask(int id, List<String> files) {
        this.id = id;
        this.files = files;
    }
}
