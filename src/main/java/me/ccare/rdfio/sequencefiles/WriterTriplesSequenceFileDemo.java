package me.ccare.rdfio.sequencefiles;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import me.ccare.rdfio.UriWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;

public class WriterTriplesSequenceFileDemo {
	
	private static final URI SUBJECT_1 = new URIImpl("<http://example.com/subject1>");
	private static final URI SUBJECT_2 = new URIImpl("<http://example.com/subject2>");
	private static final URI SUBJECT_3 = new URIImpl("<http://example.com/subject3>");

	private static final URI TITLE = new URIImpl("<http://schema.example.com/title>");
	private static final URI INDEX = new URIImpl("<http://schema.example.com/index>");
	private static final URI HISTORY = new URIImpl("<http://scheam.example.com/history>");
	
	private static final URI SUBJECT_1_HISTORY = new URIImpl("<http://example.com/subject1/history>");
	private static final URI SUBJECT_2_HISTORY = new URIImpl("<http://example.com/subject2/history>");
	private static final URI SUBJECT_3_HISTORY = new URIImpl("<http://example.com/subject3/history>");

	private static final Statement[] GROUPED_TRIPLES = {
		new StatementImpl(SUBJECT_1, TITLE, new LiteralImpl("Subject 1")),
		new StatementImpl(SUBJECT_1, INDEX, new LiteralImpl("1")),
		new StatementImpl(SUBJECT_1, HISTORY, SUBJECT_1_HISTORY),
		
		new StatementImpl(SUBJECT_2, TITLE, new LiteralImpl("Subject 2")),
		new StatementImpl(SUBJECT_2, INDEX, new LiteralImpl("2")),
		new StatementImpl(SUBJECT_2, HISTORY, SUBJECT_2_HISTORY),
		
		new StatementImpl(SUBJECT_3, TITLE, new LiteralImpl("Subject 3")),
		new StatementImpl(SUBJECT_3, INDEX, new LiteralImpl("3")),
		new StatementImpl(SUBJECT_3, HISTORY, SUBJECT_3_HISTORY),
	};
	
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(java.net.URI.create("file:///tmp/sequenceFile"), conf);
		Path path = new Path("file:///tmp/sequenceFile");
		
		Writer writer = null;
		try {
			writer = SequenceFile.createWriter(fs, conf, path, UriWritable.class, Text.class);
			
			URI currentSubject = null;
			Set<Statement> currentTriples = new HashSet<Statement>();
			for (Statement s : GROUPED_TRIPLES) {
				Resource subject = s.getSubject();
				if (subject instanceof URI) {
					if (currentSubject != null && 
							! currentSubject.equals(subject)) {
						emitTriples(writer, currentSubject, currentTriples);
					}
					currentSubject = (URI) subject;
					currentTriples.add(s);
				}
			}
			if (currentSubject != null && !currentTriples.isEmpty()) {
				emitTriples(writer, currentSubject, currentTriples);
			}
			
		} finally {
			IOUtils.closeStream(writer);
		}
		
		
	}


	private static void emitTriples(Writer writer, URI currentSubject,
			Set<Statement> currentTriples) throws IOException {
		System.out.println("DONE with " + currentSubject);
		System.out.println("GOT" + currentTriples.size());
		StringBuilder sb = new StringBuilder();
		for (Statement s : currentTriples) {
			sb.append(s.getPredicate().stringValue());
			sb.append(' ');
			sb.append(s.getObject().stringValue());
		}
		writer.append(new UriWritable(currentSubject), new Text(sb.toString()));
		currentTriples.clear();
	}
}
