import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

public class ClusterRecordWriter implements RecordWriter<Text, Text> {

	private DataOutputStream output_stream;
	private ArrayList<Text> centers;

	public ClusterRecordWriter(FSDataOutputStream output_stream)
			throws IOException {
		this.output_stream = output_stream;
		centers = new ArrayList<Text>();
	}

	public void close(Reporter arg0) throws IOException {
		output_stream.close();

		Configuration configuration = new Configuration();

		try {
			FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:8000"),
					configuration);

			Path file = new Path("hdfs://localhost:8000/Inputs/centers");
			if (hdfs.exists(file)) {
				hdfs.delete(file, true);
			}

			DataOutputStream key_output_stream = new DataOutputStream(
					hdfs.create(file, true));
			key_output_stream.writeBytes((centers.size() + "\n"));
			for (Text t : centers)
				key_output_stream.writeBytes(t.toString());

			key_output_stream.close();

			hdfs.close();

		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

	}

	public synchronized void write(Text key, Text value) throws IOException {
		output_stream.writeBytes(key.toString() + value.toString());
		centers.add(key);
	}

}
