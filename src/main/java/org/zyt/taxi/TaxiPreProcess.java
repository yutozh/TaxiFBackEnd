package org.zyt.taxi;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.zyt.taxi.MRTools.TaxiMapper;
import org.zyt.taxi.MRTools.TaxiMapper.MyGroupingComparator;
import org.zyt.taxi.MRTools.TaxiReducer;
import org.zyt.taxi.MRTools.TpWritable;
import org.zyt.taxi.Utils.RecordTime;

public class TaxiPreProcess {
	public static void main(String[] args) throws Exception {
		TaxiPreProcess.run(args);
//		TaxiInsert.insertRouteAndTp();
	}
	
	public static boolean run(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		if(args.length < 2){
			args = new String[]{
					"hdfs://master:9000/taxi/input",
					"hdfs://master:9000/taxi/output",
					"hadoop"
			};
		}
		try {
			// 删除output中已有的数据
			FileSystem fileSystem = FileSystem.get(new URI(args[0]), new Configuration(), args[2]);
			 if (fileSystem.exists(new Path(args[0]))) {  
			      fileSystem.delete(new Path(args[1]), true);  
			  }
			
//			String TableName = "taxi-route";
//			Configuration conf = HBaseConfiguration.create();
//			conf.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
////	        conf.set("hbase.zookeeper.quorum", "10.133.253.130");
////	        conf.set("hbase.zookeeper.property.clientPort", "2181");
////	        conf.set(TableOutputFormat.OUTPUT_TABLE, TableName);
	//
//			Job job = Job.getInstance(conf, "taxi");
	//
//			job.setJarByClass(TaxiPreProcess.class);
//			job.setMapperClass(TaxiMapper.class);
//			job.setReducerClass(TaxiTableReducer.class);
	//
////			job.setNumReduceTasks(1);
//			// 设置分组依据
//			job.setGroupingComparatorClass(MyGroupingComparator.class);
//			
//			FileInputFormat.setInputPaths(job, new Path(args[0]));
//			
////			TableMapReduceUtil.addDependencyJars(job); 
//			TableMapReduceUtil.initTableReducerJob(TableName, TaxiTableReducer.class, job);
	//
//			job.setMapOutputKeyClass(Text.class);
//			job.setMapOutputValueClass(TpWritable.class);
	//
//	        job.setInputFormatClass(TextInputFormat.class);  
//	        job.setOutputFormatClass(TableOutputFormat.class);  
	       
			 
			// 直接输出文件
			Configuration conf = new Configuration();
	        
			Job job = Job.getInstance(conf, "taxi");

			job.setJarByClass(TaxiPreProcess.class);
			job.setMapperClass(TaxiMapper.class);
			job.setReducerClass(TaxiReducer.class);

			// 设置分组依据
			job.setGroupingComparatorClass(MyGroupingComparator.class);
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(TpWritable.class);

//	        job.setInputFormatClass(TextInputFormat.class);  
//	        job.setOutputFormatClass(.class);  
			// Reduce 输出key，value类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			long time1=System.currentTimeMillis();
			boolean isok = job.waitForCompletion(true);
			long time2=System.currentTimeMillis();
			RecordTime.writeLocalStrOne("MR "+(time2-time1)+ "\n", "");

			if(!isok){
				System.out.print("PreProcess Failed!");
				return false;
			}else{
				System.out.println("PreProcess Succeed!");
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
}
