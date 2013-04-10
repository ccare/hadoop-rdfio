package me.ccare.rdfio.sequencefiles;

import java.io.IOException;

import me.ccare.rdfio.UriWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class ReadSequenceFile {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(java.net.URI.create("file:///tmp/sequenceFile"), conf);
		Path path = new Path("file:///tmp/sequenceFile");
		
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			UriWritable key = new UriWritable();
			Text val = new Text();
			while(reader.next(key, val)) {
				System.out.println(key.toString());
				System.out.println(val.toString());
				System.out.println("----");
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}

}
