package org.hadoop.crawler.mapReduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hadoop.crawler.model.DocumentWritable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


//Replace LongWritable with custom object which has documentId, Document keywords, doc url
public class CrawlerMapper extends Mapper<DocumentWritable, BooleanWritable, DocumentWritable, BooleanWritable>{

	public CrawlerMapper() {
		
	}
	
	@Override
    protected void map(DocumentWritable doc, BooleanWritable isDownloaded, Context context) 
    		throws IOException, InterruptedException {
		
		if(isDownloaded.get()) {
			context.write(doc, isDownloaded);
			return;
		}
		isDownloaded = downloadDoc(doc);
		parse(doc, context);
		context.write(doc, isDownloaded);
	}

	private void parse(DocumentWritable doc, Context context) throws IOException, InterruptedException {
		Document document = Jsoup.connect(doc.getDocUrl().toString()).validateTLSCertificates(false).get();
        Elements linksOnPage = document.select("a[href]");
        
        for(Element page: linksOnPage) {
        		DocumentWritable outputDoc = new DocumentWritable();
            outputDoc.setDocUrl(new String(page.attr("abs:href")));
            outputDoc.setDocUrlText(page.text());
            context.write(outputDoc, new BooleanWritable(false));
        }
	}

	private BooleanWritable downloadDoc(DocumentWritable doc) throws IOException {
		/*
		URL url = new URL(docId.toString());
        BufferedReader readr = new BufferedReader(new InputStreamReader(url.openStream()));
        BufferedWriter writer = new BufferedWriter(new FileWriter("Download.html"));
        String line;
        while ((line = readr.readLine()) != null) {
            writer.write(line);
        }
        readr.close();
        writer.close();
        System.out.println("Successfully Downloaded.");
		*/
		return new BooleanWritable(true);
	}
	

}
