import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class ChessPairPartitioner extends
  	Partitioner<ChessPairWritable, Text> {

	@Override
	public int getPartition(ChessPairWritable key, Text value,
			int numReduceTasks) {

		return (key.getDefaultKey().hashCode() % numReduceTasks);
	}
}
