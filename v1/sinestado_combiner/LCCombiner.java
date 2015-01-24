import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LCCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
	public void reduce(Text clave, Iterable<IntWritable> valores, Context contexto) throws IOException, InterruptedException {
		System.out.println("-------____________KEY: "+clave);
		String [] splits = clave.toString().split("_");	
		contexto.write(new Text(splits[0]), new IntWritable(1));
	}
}
