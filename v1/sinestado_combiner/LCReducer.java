import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LCReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	public void reduce(Text clave, Iterable<IntWritable> valores, Context contexto) throws IOException, InterruptedException {
		int sum = 0;
		System.out.println("--------------KEY: "+clave);
		for (IntWritable valor : valores)
			sum ++;
		System.out.println("--------------****VALOR: "+sum);
		contexto.write(new Text(clave), new IntWritable(sum));
	}
}
