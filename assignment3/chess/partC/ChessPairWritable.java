import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class ChessPairWritable implements Writable,
  	WritableComparable<ChessPairWritable> {

  	// all the pairs would have the same default to be mapped together
	private String defaultKey = "1";
	// number of count a certain steps number shows up
	private int count;

	public ChessPairWritable() {
	}

	public ChessPairWritable(int count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(defaultKey).append("\t")
				.append(count)).toString();
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		defaultKey = WritableUtils.readString(dataInput);
		count = WritableUtils.readVInt(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, defaultKey);
		WritableUtils.writeVInt(dataOutput, count);
	}

	@Override
	// sort in the reverse order
	public int compareTo(ChessPairWritable objKeyPair) {
		return objKeyPair.getCount() - count;
	}

	public int getCount() {
		return count;
	}

	public String getDefaultKey() {
		return defaultKey;
	}
}
