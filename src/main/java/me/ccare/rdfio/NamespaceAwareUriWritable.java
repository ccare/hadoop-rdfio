package me.ccare.rdfio;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.google.common.collect.ImmutableBiMap;

public class NamespaceAwareUriWritable implements WritableComparable<NamespaceAwareUriWritable> {

	private static enum SerialisationType {
		PLAIN,
		PREFIXED
	}

	private static final ImmutableBiMap<String, String> NAMESPACES =
			new ImmutableBiMap.Builder<String, String>()
			.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
			.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
			.put("foaf", "http://xmlns.com/foaf/0.1/")
			.build();

	private URI uri;

	public NamespaceAwareUriWritable() {
		super();
	}

	public NamespaceAwareUriWritable(URI uri) {
		this.uri = uri;
	}

	public void write(DataOutput out) throws IOException {
		String ns = uri.getNamespace();
		if (NAMESPACES.containsValue(ns)) {
			WritableUtils.writeEnum(out, SerialisationType.PREFIXED);
			String prefix = NAMESPACES.inverse().get(ns);
			WritableUtils.writeString(out, prefix);		
			WritableUtils.writeString(out, uri.getLocalName());					
		} else {
			WritableUtils.writeEnum(out, SerialisationType.PLAIN);
			WritableUtils.writeString(out, uri.stringValue());				
		}			
	}

	public void readFields(DataInput in) throws IOException {
		SerialisationType type = WritableUtils.readEnum(in, SerialisationType.class);
		if (type == SerialisationType.PREFIXED) {
			final  String prefix = WritableUtils.readString(in);
			final  String localName = WritableUtils.readString(in);
			String ns = NAMESPACES.get(prefix);
			uri = new URIImpl(ns + localName);
		} else if (type == SerialisationType.PLAIN) {
			final  String uriStr = WritableUtils.readString(in);
			uri = new URIImpl(uriStr);
		}        
	}

	@Override
	public String toString() {
		return uri.stringValue();
	}

	public int compareTo(NamespaceAwareUriWritable o) {
		return uri.stringValue().compareTo(o.uri.stringValue());
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof NamespaceAwareUriWritable))
			return false;
		NamespaceAwareUriWritable that = (NamespaceAwareUriWritable)other;
		return this.compareTo(that) == 0;
	}

	@Override
	public int hashCode() {
		return (uri == null) ? 1 : uri.hashCode();
	}

	public URI getUri() {
		return uri;
	}
	
	

}
