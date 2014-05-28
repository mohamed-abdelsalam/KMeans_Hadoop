import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;

public class KMeans {

	// 1. sepal length in cm
	// 2. sepal width in cm
	// 3. petal length in cm
	// 4. petal width in cm
	// 5. class:
	// -- Iris Setosa
	// -- Iris Versicolour
	// -- Iris Virginica

	private static URI uri;
	private static Configuration fsconfig = new Configuration();
	private static final double epc = 10E-5;

	private static boolean equal(String[] centroids, String[] new_centroids) {
		if (centroids.length != new_centroids.length)
			return false;

		for (int i = 0; i < centroids.length; i++) {
			String[] tokens = centroids[i].split(",");

			String[] new_tokens = new_centroids[i].split(",");
			for (int k = 0; k < tokens.length; k++) {
				if (Double.parseDouble(tokens[k])
						- Double.parseDouble(new_tokens[k]) >= epc) {
					return false;
				}
			}
		}
		return true;
	}

	private static String[] read_centroid_file() throws IOException {
		Path centers_path = new Path("hdfs://localhost:8000/Inputs/centers");

		FileSystem fileSystem = FileSystem.get(uri, fsconfig);
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				fileSystem.open(centers_path)));

		int K = Integer.parseInt(reader.readLine());

		String[] centroids = new String[K];
		for (int i = 0; i < K; i++) {
			centroids[i] = reader.readLine();
			centroids[i] = centroids[i].substring(
					centroids[i].indexOf('(') + 1, centroids[i].indexOf(')'));
		}
		return centroids;
	}

	public static void main(String[] args) throws IllegalArgumentException,
			IOException, ClassNotFoundException, InterruptedException,
			URISyntaxException {

		Path data_file_path = new Path("hdfs://localhost:8000/Inputs/iris.data");
		uri = new URI("hdfs://localhost:8000");

		long startTime = System.nanoTime();
		int v = 0;
		while (v < 100) {
			String[] centroids = read_centroid_file();

			JobConf conf = new JobConf(KMeans.class);

			conf.setStrings("centroids", centroids);

			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(Flower.class);

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			conf.setMapperClass(Map.class);

			conf.setReducerClass(Reduce.class);

			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(ClusterOutputFormat.class);

			FileInputFormat.setInputPaths(conf, data_file_path);

			FileSystem hdfs = FileSystem.get(uri, fsconfig);

			Path file = new Path("hdfs://localhost:8000/Inputs/xout");
			if (hdfs.exists(file)) {
				hdfs.delete(file, true);
			}
			FileOutputFormat.setOutputPath(conf, new Path(
					"hdfs://localhost:8000/Inputs/xout"));

			RunningJob job = JobClient.runJob(conf);
			job.waitForCompletion();

			String[] new_centroids = read_centroid_file();
			if (equal(centroids, new_centroids)) {
				System.out.println("End in " + v + " Iterations");
				System.out.println("Time = " + (System.nanoTime() - startTime)
						/ 1E9 + " sec.");
				break;
			}
			v++;
		}
	}
}
