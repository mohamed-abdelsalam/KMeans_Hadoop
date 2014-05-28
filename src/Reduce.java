import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

// <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
public class Reduce extends MapReduceBase implements
		Reducer<Text, Flower, Text, Text> {

	private double[] calculate_new_means(ArrayList<Flower> points) {

		double[] means = { 0.0, 0.0, 0.0, 0.0 };

		for (Flower f : points) {
			means[0] += f.sepal_length;
			means[1] += f.sepal_width;
			means[2] += f.petal_length;
			means[3] += f.petal_width;
		}

		for (int i = 0; i < means.length; i++)
			means[i] /= points.size();

		return means;
	}

	public void reduce(Text key_in, Iterator<Flower> val_in,
			OutputCollector<Text, Text> output, Reporter arg3)
			throws IOException {

		System.out.println(key_in);

		ArrayList<Flower> points_in_cluster = new ArrayList<Flower>();

		while (val_in.hasNext()) {
			Flower f = val_in.next();
			points_in_cluster.add(new Flower(f));
		}
		double[] means = calculate_new_means(points_in_cluster);

		NumberFormat formatter = new DecimalFormat("#0.00000");

		String centroid = formatter.format(means[0]);

		for (int i = 1; i < means.length; i++)
			centroid += "," + formatter.format(means[i]);

		Text new_centeroid = new Text(
				("Number of points " + points_in_cluster.size() + " Centroid ("
						+ centroid + ").\n").getBytes());

		Text points = new Text();

		for (Flower f : points_in_cluster) {
			byte[] point = (f.toString() + '\n').getBytes();
			points.append(point, 0, point.length);
		}

		output.collect(new_centeroid, points);
	}
}

// StringTokenizer token = new StringTokenizer(last_centroid.toString());
// double last_sepal_length_mean = Double.parseDouble(token.nextToken());
// double last_sepal_width_mean = Double.parseDouble(token.nextToken());
//
// double last_petal_length_mean = Double.parseDouble(token.nextToken());
// double last_petal_width_mean = Double.parseDouble(token.nextToken());
//
// if (Math.abs(sepal_length_mean - last_sepal_length_mean) <= Epc
// && Math.abs(sepal_width_mean - last_sepal_width_mean) <= Epc
// && Math.abs(petal_length_mean - last_petal_length_mean) <= Epc
// && Math.abs(petal_width_mean - last_petal_width_mean) <= Epc) {
//
// }