package org.hadoop.crawler.indexer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hadoop.crawler.model.DocumentWritable;

public class IndexerMapper extends Mapper<Text, DocumentWritable, Text, DocumentWritable> {

	public IndexerMapper() {
		// TODO Auto-generated constructor stub
	}
	
	@Override
    protected void map(Text keyword, DocumentWritable doc, Context context) throws IOException, InterruptedException{
		
		stringtokenizer
		
	}
	
	

}
