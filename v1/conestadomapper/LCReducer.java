import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashMap;
import java.util.Map;

public class LCReducer extends Reducer<Text,Text,Text,IntWritable> {
	public void reduce(Text clave, Iterable<Text> valores, Context contexto) throws IOException, InterruptedException {
		System.out.println("reduce: "+clave+"********************");
		HashMap<String, Integer> tabla = new HashMap<String, Integer>();
		int total=0;
		for (Text valor : valores) {
			System.out.println("-----c: "+clave+" ----v: "+valor);
			total++;
		}
		/*for(Map.Entry<String, Integer> palabra : tabla.entrySet())
			System.out.println("-----c: "+clave+" ----w: "+palabra.getKey()+" ----k: "+palabra.getValue());   */  
		System.out.println("-----c: "+clave+" ----total: "+total);
		contexto.write(clave, new IntWritable(total));
	}
}
