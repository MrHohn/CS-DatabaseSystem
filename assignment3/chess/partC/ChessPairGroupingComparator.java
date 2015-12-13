import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ChessPairGroupingComparator extends WritableComparator {
  protected ChessPairGroupingComparator() {
		super(ChessPairWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		ChessPairWritable key1 = (ChessPairWritable) w1;
		ChessPairWritable key2 = (ChessPairWritable) w2;
		return key1.getDefaultKey().compareTo(key2.getDefaultKey());
	}
}
