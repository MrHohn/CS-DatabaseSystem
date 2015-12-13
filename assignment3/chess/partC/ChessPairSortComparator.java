import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class ChessPairSortComparator extends WritableComparator {

  protected ChessPairSortComparator() {
		super(ChessPairWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		ChessPairWritable key1 = (ChessPairWritable) w1;
		ChessPairWritable key2 = (ChessPairWritable) w2;

		return key1.compareTo(key2);
	}
}