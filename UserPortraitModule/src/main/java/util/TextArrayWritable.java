package util;

import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class TextArrayWritable extends ArrayWritable {
	public TextArrayWritable() {
		super(Text.class);
	}

	public TextArrayWritable(String[] strings) {
		super(Text.class);
		Text[] texts = new Text[strings.length];
		for (int i = 0; i < strings.length; i++) {
			texts[i] = new Text(strings[i]);
		}
		set(texts);
	}

	public TextArrayWritable(ArrayList<String> strings) {
		super(Text.class);

		Text[] texts = new Text[strings.size()];
		int i = 0;
		for (String str : strings) {
			texts[i] = new Text(str);
			i++;
		}
		set(texts);
	}

	public ArrayList<String> toArrayList(String[] writables) {
		ArrayList<String> arraylist = new ArrayList<String>();
		for (String writable : writables) {
			arraylist.add(writable.toString());
		}
		return arraylist;
	}

	public ArrayList<String> toArrayList() {
		return toArrayList(super.toStrings());
	}

}