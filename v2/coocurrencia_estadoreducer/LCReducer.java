import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashMap;
import java.util.Map;

public class LCReducer extends Reducer<Text,TextIntPair,Text,TextIntPair> {
	public void reduce(Text clave, Iterable<TextIntPair> valores, Context contexto) throws IOException, InterruptedException {
		System.out.println("reduce: "+clave+"********************");
		HashMap<String, Integer> tabla = new HashMap<String, Integer>();
		int total=0;
		for (TextIntPair valor : valores) {
			String palabra = valor.getFirst().toString();
			Integer c = tabla.get(palabra);
            if (c==null)
                    c = new Integer(1);
            else
                    c++;
            tabla.put(new String(palabra), c);
			System.out.println("-----w: "+palabra+" ----c: "+c);
		}
		/*for(Map.Entry<String, Integer> palabra : tabla.entrySet())
			System.out.println("-----c: "+clave+" ----w: "+palabra.getKey()+" ----k: "+palabra.getValue());   */  
		System.out.println("-----c: "+clave+" ----total: "+tabla.size());
		contexto.write(clave, new TextIntPair("", tabla.size()));
	}
}
