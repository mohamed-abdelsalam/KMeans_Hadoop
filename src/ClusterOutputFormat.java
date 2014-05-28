import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class ClusterOutputFormat extends FileOutputFormat<Text, Text> {

	public RecordWriter<Text, Text> getRecordWriter(FileSystem arg0,
			JobConf job, String name, Progressable progressable)
			throws IOException {
		Path output_path = FileOutputFormat.getTaskOutputPath(job, name);
		FileSystem fs = output_path.getFileSystem(job);
		FSDataOutputStream stream = fs.create(output_path, progressable);
		return new ClusterRecordWriter(stream);
	}

}
