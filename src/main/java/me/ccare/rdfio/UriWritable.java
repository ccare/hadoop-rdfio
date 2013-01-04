package me.ccare.rdfio;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.google.common.collect.ImmutableBiMap;

public class UriWritable implements WritableComparable<UriWritable> {

	private URI uri;

	public UriWritable() {
		super();
	}

	public UriWritable(URI uri) {
		this.uri = uri;
	}

	public void write(DataOutput out) throws IOException {
		String ns = uri.getNamespace();
		WritableUtils.writeString(out, uri.stringValue());		
	}

	public void readFields(DataInput in) throws IOException {
		final  String uriStr = WritableUtils.readString(in);
		uri = new URIImpl(uriStr);
	}

	@Override
	public String toString() {
		return uri.stringValue();
	}

	public int compareTo(UriWritable o) {
		return uri.stringValue().compareTo(o.uri.stringValue());
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof UriWritable))
			return false;
		UriWritable that = (UriWritable)other;
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
