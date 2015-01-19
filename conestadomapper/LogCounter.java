import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class LogCounter extends Configured implements Tool {
        public int run(String[] args) throws Exception {
		if (args.length < 2) {
      			System.err.println("Uso: logcount <in> [<in>...] <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(getConf());

		job.setJarByClass(getClass());

		job.setMapperClass(LCMapper.class);
		//job.setCombinerClass(LCCombiner.class);
		job.setReducerClass(LCReducer.class);
		
		
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		for (int i = 0; i < args.length-1; ++i)
			FileInputFormat.addInputPath(job, new Path(args[i]));
    		FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
        public static void main(String[] args) throws Exception {
                int resultado = ToolRunner.run(new LogCounter(), args);
                System.exit(resultado);
        }
}
