import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

enum incidencias {
	LINEAS_VACIAS,
	EXCLUIDOS,
	NO_GET
}

public class LCMapper extends Mapper<LongWritable, Text, Text, Text>{
	private String aExcluir;
	private HashMap<String,List<String>> map;
	public void setup(Context contexto) throws IOException, InterruptedException {
		aExcluir = contexto.getConfiguration().get("excluido");
		System.out.println("--------------"+aExcluir);
		map = new HashMap<>();
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
						System.out.println("dir: "+splits[6]+" | userid: "+splits[0]);
						List<String> lista = map.get(splits[6]);
						if(lista==null){
							System.out.println("null");
							lista = new ArrayList<String>();
							lista.add(splits[0]);
							map.put(splits[6], lista);
							contexto.write(new Text(splits[6]), new Text(splits[0]));
						} else
							if (!lista.contains(splits[0])){
								lista.add(splits[0]);
								map.put(splits[6], lista);
								contexto.write(new Text(splits[6]), new Text(splits[0]));
							}	
							
					} else 
						contexto.getCounter(incidencias.NO_GET).increment(1);
		}
	}
	public void cleanup(Context contexto) throws IOException, InterruptedException {
		return; // no hace nada; sólo por motivos pedagógicos
	}
}
  
