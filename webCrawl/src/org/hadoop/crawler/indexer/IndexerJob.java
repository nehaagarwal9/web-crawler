package org.hadoop.crawler.indexer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class IndexerJob {

	public IndexerJob() {
		// TODO Auto-generated constructor stub
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	
		int iteration = 1;
	      Configuration conf = new Configuration();
	      conf.set("num.iteration", iteration + "");
	      
	      Path inMapper = new Path("webCrawler/files/seedLinksMapper");
	      Path outReducer = new Path("webCrawler/files/outputlinks_1");
	      
	      Job job = Job.getInstance(conf);
	      job.setJobName("Indexer");
	      
	      job.setMapperClass(IndexerMapper.class);
	      job.setReducerClass(IndexerRedussscer.class);
	      job.setJarByClass(IndexerMapper.class);
	       
	      FileInputFormat.addInputPath(job, inMapper);
	      FileSystem fs = FileSystem.get(conf);
	      if (fs.exists(outReducer)) {
	    	  	fs.delete(outReducer, true);
	      }


	      if (fs.exists(inMapper)) {
	    	  	fs.delete(inMapper, true);
	      }
	      
	      writeLinks(conf, inMapper, fs);
	      
	      FileOutputFormat.setOutputPath(job, outReducer);
	      job.setInputFormatClass(SequenceFileInputFormat.class);
	      job.setOutputFormatClass(SequenceFileOutputFormat.class);
	      
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(BooleanWritable.class);
	      
	      job.waitForCompletion(true);
	      
	      
	      displayResults(conf, fs, outReducer);
	      
	      //long counter = job.getCounters().findCounter(crawlerReducer.Counter.CONVERGED).getValue();
	      //iteration++;
	     /* while (counter > 0) {
	    	  	conf = new Configuration();
	        conf.set("num.iteration", iteration + "");
	        job = Job.getInstance(conf);
	        job.setJobName("Web Crawler" + iteration);
	        
	        job.setMapperClass(crawlerMapper.class);
		    job.setReducerClass(crawlerReducer.class);
		    //job.setJarByClass(crawlerMapper.class);
		    
		    inMapper = new Path("webCrawler/files/outputlinks_" + (iteration - 1) + "/");
		    outReducer = new Path("webCrawler/files/outputlinks_" + iteration);
          
          FileInputFormat.addInputPath(job, inMapper);
          if (fs.exists(outReducer))
              fs.delete(outReducer, true);
          
          FileOutputFormat.setOutputPath(job, outReducer);
          	job.setInputFormatClass(TextInputFormat.class);
	      	job.setOutputFormatClass(TextOutputFormat.class);
	      	job.setOutputKeyClass(Text.class);
	      	job.setOutputValueClass(Text.class);
	      	
	      	job.waitForCompletion(true);
	      	iteration++;
	      	counter = job.getCounters().findCounter(crawlerReducer.Counter.CONVERGED).getValue();
	      }
	      
	      Path result = new Path("webCrawler/files/outputlinks_" + (iteration - 1) + "/");
	      
	        FileStatus[] stati = fs.listStatus(result);
	        for (FileStatus status : stati) {
	            if (!status.isDirectory()) {
	                Path path = status.getPath();
	                if (!path.getName().equals("_SUCCESS")) {
//	                    try (Text.Reader reader = new Text.Reader(fs, path, conf)) {
//
//	                        Text t = new Text();
//	                        while (reader.next(key, t)) {
//	                            System.out.println( key + " /" + t);
//
//	                        }
//	                    }
	                }
	            }
	        }*/
          
          
	      
	}

	@SuppressWarnings("deprecation")
	private static void writeLinks(Configuration conf, Path inMapper, FileSystem fs) throws IOException {
      try(
		SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf, inMapper, Text.class,
              BooleanWritable.class)) {
          FSDataInputStream inputStream = fs.open(new Path("webCrawler/files/seedLinks.txt"));
          String data = inputStream.readLine();
          while (data != null) {
              dataWriter.append(new Text(data), new BooleanWritable(false));
              data = inputStream.readLine();
          }
          inputStream.close();
      }
		
	}
	
	private static void displayResults(Configuration conf, FileSystem fs, Path out) throws IOException {
      FileStatus[] stati = fs.listStatus(out);
      for (FileStatus status : stati) {
          if (!status.isDirectory()) {
              Path path = status.getPath();
              if (!path.getName().equals("_SUCCESS")) {
                  try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf)) {
                      Text key = new Text();
                      BooleanWritable v = new BooleanWritable();
                      while (reader.next(key, v)) {
                              System.out.println(key + " --> " + v);
                      }
                  }
              }
          }
      }
	}

}
