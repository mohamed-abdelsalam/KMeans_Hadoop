import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Flower implements Writable {

	double sepal_length, sepal_width, petal_length, petal_width;
	String type;

	public Flower() {
	}

	public Flower(Flower f) {
		sepal_length = f.sepal_length;
		sepal_width = f.sepal_width;
		petal_length = f.petal_length;
		petal_width = f.petal_width;
		type = f.type;
	}

	public Flower(double sepal_length, double sepal_width, double petal_length,
			double petal_width, String type) {

		this.sepal_length = sepal_length;
		this.sepal_width = sepal_width;

		this.petal_length = petal_length;
		this.petal_width = petal_width;

		this.type = type;

	}

	public String toString() {
		return sepal_length + "," + sepal_width + "," + petal_length + ","
				+ petal_width + "," + type;
	}

	public void readFields(DataInput in_stream) throws IOException {

		sepal_length = in_stream.readDouble();
		sepal_width = in_stream.readDouble();

		petal_length = in_stream.readDouble();
		petal_width = in_stream.readDouble();

		type = in_stream.readLine();

	}

	public void write(DataOutput out_stream) throws IOException {

		out_stream.writeDouble(sepal_length);
		out_stream.writeDouble(sepal_width);
		out_stream.writeDouble(petal_length);
		out_stream.writeDouble(petal_width);

		out_stream.writeBytes(type + '\n');
	}

}
