package me.ccare.rdfio;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

public class UriWritableComparator extends WritableComparator {
	
	private static final Text.Comparator TEXT_COMP = new Text.Comparator();

	public UriWritableComparator() {
		super(UriWritable.class);
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2,
			int l2) {
		return TEXT_COMP.compare(b1, s1, l1, b2, s2, l2);
	}
	
}