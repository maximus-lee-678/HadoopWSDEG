
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hello world!
 *
 */
public class HadoopProjectSetup 
{
    public static void main( String[] args ) throws Exception
    {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HadoopProjectSetup");
		
		job.setJarByClass(HadoopProjectSetup.class);
		job.setMapperClass(ReviewMapper.class);
//		job.setCombinerClass(ReviewReducer.class);
		job.setReducerClass(ReviewReducer.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

//		Path inPath = new Path("hdfs://localhost:9000/user/phamvanvung/project/input/");
//		Path outPath = new Path("hdfs://localhost:9000/user/phamvanvung/project/output");
		Path inPath = new Path("hdfs://hadoop-master:9000/user/ict2100724/project/input/");
		Path outPath = new Path("hdfs://hadoop-master:9000/user/ict2100724/project/output");
		outPath.getFileSystem(conf).delete(outPath, true);
		
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

