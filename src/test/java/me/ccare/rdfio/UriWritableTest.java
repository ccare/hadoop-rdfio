package me.ccare.rdfio;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.WriteAbortedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

public class UriWritableTest {

	@Test
	public void shouldSerialiseAndDeserialise() throws Exception {
		final URI uri = new URIImpl("http://ccare.me#test");
		final UriWritable uriWritable = new UriWritable(uri);
		
		byte[] bytes = serialise(uriWritable);				
		final UriWritable deserialised = deserialise(bytes);
		
		assertEquals(uriWritable, deserialised);		
	}

	@Test
	public void canClone() throws Exception {
		final URI uri = new URIImpl("http://ccare.me#test");
		final UriWritable source = new UriWritable(uri);
		
		UriWritable cloned = WritableUtils.clone(source, new Configuration());
		
		assertEquals(source, cloned);	
		assertEquals(source.getUri(), cloned.getUri());		
	}
	
	@Test
	public void shouldSerialiseAndDeserialiseWellKnownPredicate() throws Exception {
		final URI uri = new URIImpl("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
		final UriWritable uriWritable = new UriWritable(uri);
		
		byte[] bytes = serialise(uriWritable);				
		final UriWritable deserialised = deserialise(bytes);
		
		assertEquals(uriWritable, deserialised);		
	}

	@Test
	public void shouldEqualSameUri() throws Exception {
		final URI a = new URIImpl("http://ccare.me#test");
		final URI b = new URIImpl("http://ccare.me#test");
		
		assertEquals( new UriWritable(a),  new UriWritable(b));		
	}

	@Test
	public void shouldNotEqualDifferentUri() throws Exception {
		final URI a = new URIImpl("http://ccare.me#test");
		final URI b = new URIImpl("http://ccare.me/other");
		
		assertNotEquals( new UriWritable(a),  new UriWritable(b));	
	}
	
	@Test
	public void shouldCompareCorrectly() throws Exception {
		final UriWritable a = new UriWritable(new URIImpl("http://ccare.me#test"));
		final UriWritable b = new UriWritable(new URIImpl("http://ccare.me#test"));
		final UriWritable c = new UriWritable(new URIImpl("http://ccare.me#zzzz"));
		
		assertTrue(a.compareTo(b) == 0);
		assertTrue(b.compareTo(a) == 0);
		assertTrue(a.compareTo(c) < 0);
		assertTrue(c.compareTo(b) > 0);
	}

	
	@Test
	public void shouldCompareCorrectlyUsingRawComparator() throws Exception {
		final UriWritable a = new UriWritable(new URIImpl("http://aaa.com"));
		final UriWritable b = new UriWritable(new URIImpl("http://vvv.com"));
		final UriWritable c = new UriWritable(new URIImpl("http://www.w3.org/"));
		final UriWritable d = new UriWritable(new URIImpl("http://www.w3.org/1999/02/22-rdf-syntax-ns#type1"));
		final UriWritable e = new UriWritable(new URIImpl("http://www.w3.org/1999/02/22-rdf-syntax-ns#type2"));
		
		assertTrue(a.compareTo(b) < 0);
		assertTrue(b.compareTo(c) < 0);
		assertTrue(c.compareTo(d) < 0);
		assertTrue(d.compareTo(e) < 0);
		
		byte[] bytesA = serialise(a);
		byte[] bytesB = serialise(b);
		byte[] bytesC = serialise(c);
		byte[] bytesD = serialise(d);
		byte[] bytesE = serialise(e);
		
		WritableComparator comp = new UriWritableComparator();
		
		assertTrue(comp.compare(a, b) < 0);		
		assertTrue(comp.compare(b, c) < 0);		
		assertTrue(comp.compare(c, d) < 0);		
		assertTrue(comp.compare(d, e) < 0);

		assertLessThan(comp, bytesA, bytesB);	
		assertLessThan(comp, bytesB, bytesC);	
		assertLessThan(comp, bytesC, bytesD);	
		assertLessThan(comp, bytesD, bytesE);

		assertGreaterThan(comp, bytesB, bytesA);	
		assertGreaterThan(comp, bytesC, bytesB);	
		assertGreaterThan(comp, bytesD, bytesC);	
		assertGreaterThan(comp, bytesE, bytesD);

		assertTrue(comp.compare(bytesA, 0, bytesA.length, bytesA, 0, bytesA.length) == 0);
	}

	private void assertLessThan(WritableComparator comp, byte[] a, byte[] b) {
		assertTrue(comp.compare(a, 0, a.length, b, 0, b.length) < 0);
	}

	private void assertGreaterThan(WritableComparator comp, byte[] a, byte[] b) {
		assertTrue(comp.compare(a, 0, a.length, b, 0, b.length) > 0);
	}

	private UriWritable deserialise(byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);	
		final UriWritable deserialised = new UriWritable();
		deserialised.readFields(dataIn);
		return deserialised;
	}

	private byte[] serialise(final UriWritable uriWritable) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		uriWritable.write(dataOut);
		dataOut.close();
		byte[] bytes = out.toByteArray();
		return bytes;
	}
}
