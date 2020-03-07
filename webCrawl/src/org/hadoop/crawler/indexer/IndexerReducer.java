package org.hadoop.crawler.indexer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.hadoop.crawler.model.DocumentWritable;

public class IndexerReducer extends Reducer<Text, DocumentWritable, Text, DocumentWritable>{
	
	public IndexerReducer() {
		// TODO Auto-generated constructor stub
	}
	
	protected void reduce(Text keyword, Iterable<DocumentWritable> doc, Context context) throws IOException,
    InterruptedException {
		
	}

}
