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

public class NamespaceAwareUriWritableTest {

	@Test
	public void shouldSerialiseAndDeserialise() throws Exception {
		final URI uri = new URIImpl("http://ccare.me#test");
		final NamespaceAwareUriWritable NamespaceAwareUriWritable = new NamespaceAwareUriWritable(uri);
		
		byte[] bytes = serialise(NamespaceAwareUriWritable);				
		final NamespaceAwareUriWritable deserialised = deserialise(bytes);
		
		assertEquals(NamespaceAwareUriWritable, deserialised);		
	}

	@Test
	public void canClone() throws Exception {
		final URI uri = new URIImpl("http://ccare.me#test");
		final NamespaceAwareUriWritable source = new NamespaceAwareUriWritable(uri);
		
		NamespaceAwareUriWritable cloned = WritableUtils.clone(source, new Configuration());
		
		assertEquals(source, cloned);	
		assertEquals(source.getUri(), cloned.getUri());		
	}
	
	@Test
	public void shouldSerialiseAndDeserialiseWellKnownPredicate() throws Exception {
		final URI uri = new URIImpl("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
		final NamespaceAwareUriWritable NamespaceAwareUriWritable = new NamespaceAwareUriWritable(uri);
		
		byte[] bytes = serialise(NamespaceAwareUriWritable);				
		final NamespaceAwareUriWritable deserialised = deserialise(bytes);
		
		assertEquals(NamespaceAwareUriWritable, deserialised);		
	}

	@Test
	public void shouldEqualSameUri() throws Exception {
		final URI a = new URIImpl("http://ccare.me#test");
		final URI b = new URIImpl("http://ccare.me#test");
		
		assertEquals( new NamespaceAwareUriWritable(a),  new NamespaceAwareUriWritable(b));		
	}

	@Test
	public void shouldNotEqualDifferentUri() throws Exception {
		final URI a = new URIImpl("http://ccare.me#test");
		final URI b = new URIImpl("http://ccare.me/other");
		
		assertNotEquals( new NamespaceAwareUriWritable(a),  new NamespaceAwareUriWritable(b));	
	}
	
	@Test
	public void shouldCompareCorrectly() throws Exception {
		final NamespaceAwareUriWritable a = new NamespaceAwareUriWritable(new URIImpl("http://ccare.me#test"));
		final NamespaceAwareUriWritable b = new NamespaceAwareUriWritable(new URIImpl("http://ccare.me#test"));
		final NamespaceAwareUriWritable c = new NamespaceAwareUriWritable(new URIImpl("http://ccare.me#zzzz"));
		
		assertTrue(a.compareTo(b) == 0);
		assertTrue(b.compareTo(a) == 0);
		assertTrue(a.compareTo(c) < 0);
		assertTrue(c.compareTo(b) > 0);
	}

	
//	@Test
//	public void shouldCompareCorrectlyUsingRawComparator() throws Exception {
//		final NamespaceAwareUriWritable a = new NamespaceAwareUriWritable(new URIImpl("http://aaa.com"));
//		final NamespaceAwareUriWritable b = new NamespaceAwareUriWritable(new URIImpl("http://vvv.com"));
//		final NamespaceAwareUriWritable c = new NamespaceAwareUriWritable(new URIImpl("http://www.w3.org/"));
//		final NamespaceAwareUriWritable d = new NamespaceAwareUriWritable(new URIImpl("http://www.w3.org/1999/02/22-rdf-syntax-ns#type1"));
//		final NamespaceAwareUriWritable e = new NamespaceAwareUriWritable(new URIImpl("http://www.w3.org/1999/02/22-rdf-syntax-ns#type2"));
//		
//		assertTrue(a.compareTo(b) < 0);
//		assertTrue(b.compareTo(c) < 0);
//		assertTrue(c.compareTo(d) < 0);
//		assertTrue(d.compareTo(e) < 0);
//		
//		byte[] bytesA = serialise(a);
//		byte[] bytesB = serialise(b);
//		byte[] bytesC = serialise(c);
//		byte[] bytesD = serialise(d);
//		byte[] bytesE = serialise(e);
//		
//		WritableComparator comp = new WritableComparator(NamespaceAwareUriWritable.class) {
//
//			@Override
//			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2,
//					int l2) {
//				
//			}
//			
//		};
//		
//		assertTrue(comp.compare(a, b) < 0);		
//		assertTrue(comp.compare(b, c) < 0);		
//		assertTrue(comp.compare(c, d) < 0);		
//		assertTrue(comp.compare(d, e) < 0);
//
//		assertTrue(comp.compare(bytesA, 0, bytesA.length, bytesB, 0, bytesB.length) < 0);	
//		assertTrue(comp.compare(bytesB, 0, bytesB.length, bytesA, 0, bytesA.length) > 0);	
//		
//		
//		
//		
//		
//	}

	private NamespaceAwareUriWritable deserialise(byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);	
		final NamespaceAwareUriWritable deserialised = new NamespaceAwareUriWritable();
		deserialised.readFields(dataIn);
		return deserialised;
	}

	private byte[] serialise(final NamespaceAwareUriWritable NamespaceAwareUriWritable) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		NamespaceAwareUriWritable.write(dataOut);
		dataOut.close();
		byte[] bytes = out.toByteArray();
		return bytes;
	}
}
