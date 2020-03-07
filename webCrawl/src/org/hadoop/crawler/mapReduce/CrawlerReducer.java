package org.hadoop.crawler.mapReduce;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.hadoop.crawler.model.DocumentWritable;


public class CrawlerReducer extends Reducer<DocumentWritable, BooleanWritable, DocumentWritable, BooleanWritable>{

	public CrawlerReducer() {
		
	}
	
	protected void reduce(DocumentWritable doc, Iterable<BooleanWritable> isDownloaded, Context context) throws IOException,
            InterruptedException {
		
		BooleanWritable downloaded = new BooleanWritable(false);
		for(BooleanWritable download : isDownloaded) {
			downloaded.set(downloaded.get() || download.get());
		}
		context.write(doc, downloaded);
	}

}
