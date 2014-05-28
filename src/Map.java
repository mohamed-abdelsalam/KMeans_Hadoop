import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

//<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
public class Map extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Flower> {

	private double[][] centroids;

	private String[] centroids_strings;

	public void configure(JobConf job) {

		String[] centroids_from_conf = job.getStrings("centroids");
		centroids_strings = new String[centroids_from_conf.length / 4];

		centroids = new double[centroids_strings.length][4];
		int c = 0;
		for (int i = 0; i < centroids_strings.length; i++) {
			centroids[i][0] = Double.parseDouble(centroids_from_conf[c++]);
			centroids_strings[i] = "" + centroids[i][0];
			for (int j = 1; j < 4; j++) {
				centroids[i][j] = Double.parseDouble(centroids_from_conf[c++]);
				centroids_strings[i] += "," + centroids[i][j];
			}
		}

	}

	private double compute_dist(double sepal_length, double sepal_width,
			double petal_length, double petal_width,
			double sepal_length_center, double sepal_width_center,
			double petal_length_center, double petal_width_center) {

		return Math.sqrt(Math.pow((sepal_length - sepal_length_center), 2)
				+ Math.pow((sepal_width - sepal_width_center), 2)
				+ Math.pow((petal_length - petal_length_center), 2)
				+ Math.pow((petal_width - petal_width_center), 2));

	}

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Flower> output, Reporter reporter)
			throws IOException {

		String[] items = value.toString().split(",");
		if (items.length == 5) {

			double sepal_length = Double.parseDouble(items[0]);
			double sepal_width = Double.parseDouble(items[1]);
			double petal_length = Double.parseDouble(items[2]);
			double petal_width = Double.parseDouble(items[3]);

			Double min = Double.MAX_VALUE;
			Integer minIndex = 0;

			for (int j = 0; j < centroids.length; j++) {
				double dist = compute_dist(sepal_length, sepal_width,
						petal_length, petal_width, centroids[j][0],
						centroids[j][1], centroids[j][2], centroids[j][3]);
				if (dist < min) {
					min = dist;
					minIndex = j;
				}
			}
			output.collect(new Text(centroids_strings[minIndex].getBytes()),
					new Flower(sepal_length, sepal_width, petal_length,
							petal_width, items[4]));
		}

	}
}
