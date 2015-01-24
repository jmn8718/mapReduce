import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.StringTokenizer;
 
enum incidencias {
	LINEAS_VACIAS,
	EXCLUIDOS,
	NO_GET
}

public class LCMapper extends Mapper<LongWritable, Text, Text, TextIntPair>{
	private String aExcluir;

	public void setup(Context contexto) throws IOException, InterruptedException {
		aExcluir = contexto.getConfiguration().get("excluido");
		System.out.println("--------------"+aExcluir);
	}

	public void map(LongWritable clave, Text linea, Context contexto) throws IOException, InterruptedException {
		//System.out.println("--------------MAP: "+linea+"--------------MAP");
		if (linea.getLength() == 0)
			contexto.getCounter(incidencias.LINEAS_VACIAS).increment(1);
		else {
				String [] splits = linea.toString().split(" ");
				if(splits[0].equals(aExcluir))					
					contexto.getCounter(incidencias.EXCLUIDOS).increment(1);
				else
					if(splits[5].contains("GET")){
						System.out.println("dir: "+splits[0]+" | userid: "+splits[2]);
						contexto.write(new Text(splits[0]), new TextIntPair(splits[2], 1));	
					} else 
						contexto.getCounter(incidencias.NO_GET).increment(1);
		}
	}
	public void cleanup(Context contexto) throws IOException, InterruptedException {
		return; // no hace nada; sólo por motivos pedagógicos
	}
}
  
