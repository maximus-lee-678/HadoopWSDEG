
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import java.net.URI;

/**
 * Hello world!
 *
 */
public class HadoopProjectSetup {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HadoopProjectSetup");

		Configuration validationConf = new Configuration(false);
		ChainMapper.addMapper(job, ReviewValidationMapper.class, LongWritable.class, Text.class, Text.class, Text.class,
				validationConf);

		Configuration ansConf = new Configuration(false);
		ChainMapper.addMapper(job, ReviewMapper.class, Text.class, Text.class, Text.class, Text.class, ansConf);

		// Put this file to distributed cache so we can use it
//		job.addCacheFile(new URI("hdfs://localhost:9000/user/phamvanvung/project/AFINN-111.txt"));
//		job.addCacheFile(new URI("hdfs://localhost:9000/user/phamvanvung/project/stopwords.txt"));
		job.addCacheFile(new URI("hdfs://hadoop-master:9000/user/ict2100724/project/AFINN-111.txt"));
		job.addCacheFile(new URI("hdfs://hadoop-master:9000/user/ict2100724/project/stopwords.txt"));
		
		job.setJarByClass(HadoopProjectSetup.class);
		job.setMapperClass(ChainMapper.class);
		// job.setCombinerClass(ReviewReducer.class);
		job.setReducerClass(ReviewReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

//		 Path inPath = new
//		 Path("hdfs://localhost:9000/user/phamvanvung/project/input/");
//		 Path outPath = new
//		 Path("hdfs://localhost:9000/user/phamvanvung/project/output");
		Path inPath = new Path("hdfs://hadoop-master:9000/user/ict2100724/project/input/");
		Path outPath = new Path("hdfs://hadoop-master:9000/user/ict2100724/project/output");
		outPath.getFileSystem(conf).delete(outPath, true);

		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
