package org.hadoop.crawler.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public final class DocumentWritable implements WritableComparable<DocumentWritable> {
	
	private String docUrl;
	
	private String docUrlText;
	

	public String getDocUrl() {
		return docUrl;
	}

	public void setDocUrl(String docUrl) {
		this.docUrl = docUrl;
	}

	public String getDocUrlText() {
		return docUrlText;
	}

	public void setDocUrlText(String docUrlText) {
		this.docUrlText = docUrlText;
	}

	public DocumentWritable() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(DocumentWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
