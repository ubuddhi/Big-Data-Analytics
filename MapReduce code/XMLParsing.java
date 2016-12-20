package parsing;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class XMLParsing {
	public static Properties properties;
	public static Set<Object> propConf;
	//public static Set<String> propSet = new HashSet<String>();
	
	public static class XmlInputFormat1 extends TextInputFormat {
		public static final String START_TAG_KEY = "xmlinput.start";
		public static final String END_TAG_KEY = "xmlinput.end";

		public RecordReader<LongWritable, Text> createRecordReader(
				InputSplit split, TaskAttemptContext context) {
			return new XmlRecordReader();
		}

		public static class XmlRecordReader extends
				RecordReader<LongWritable, Text> {
			private byte[] startTag;
			private byte[] endTag;
			private long start;
			private long end;
			//private String[] prop;
			private FSDataInputStream fsin;
			private DataOutputBuffer buffer = new DataOutputBuffer();
			private LongWritable key = new LongWritable();
			private Text value = new Text();

			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				this.startTag = conf.get("xmlinput.start").getBytes("utf-8");
				this.endTag = conf.get("xmlinput.end").getBytes("utf-8");
				FileSplit fileSplit = (FileSplit) split;
				this.start = fileSplit.getStart();
				this.end = (this.start + fileSplit.getLength());
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(conf);
				this.fsin = fs.open(fileSplit.getPath());
				this.fsin.seek(this.start);
				/*this.prop = conf.getStrings("propetries");
				propConf = new ArrayList<String>(Arrays.asList(prop));*/

			}

			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				if ((this.fsin.getPos() < this.end)
						&& (readUntilMatch(this.startTag, false))) {
					try {
						this.buffer.write(this.startTag);
						if (readUntilMatch(this.endTag, true)) {
							this.key.set(this.fsin.getPos());
							this.value.set(this.buffer.getData(), 0,
									this.buffer.getLength());
							return true;
						}
					} finally {
						this.buffer.reset();
					}
					this.buffer.reset();

					this.buffer.reset();
				}
				return false;
			}

			public LongWritable getCurrentKey() throws IOException,
					InterruptedException {
				return this.key;
			}

			public Text getCurrentValue() throws IOException,
					InterruptedException {
				return this.value;
			}

			public void close() throws IOException {
				this.fsin.close();
			}

			public float getProgress() throws IOException {
				return (float) (this.fsin.getPos() - this.start)
						/ (float) (this.end - this.start);
			}

			private boolean readUntilMatch(byte[] match, boolean withinBlock)
					throws IOException {
				int i = 0;
				do {
					int b = this.fsin.read();
					if (b == -1) {
						return false;
					}
					if (withinBlock) {
						this.buffer.write(b);
					}
					if (b == match[i]) {
						i++;
						if (i >= match.length) {
							return true;
						}
					} else {
						i = 0;
					}
				} while ((withinBlock) || (i != 0)
						|| (this.fsin.getPos() < this.end));
				return false;
			}
		}
	}
	
	//Mapper - Must initialize variable in setup
	//Override setup and cleanup..
	//Setup and cleanup runs everytime new map is created

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		//MultipleOutputs<Text, IntWritable> mos;
		static Set<Object> alist = new HashSet<Object>();
		/*Object[] obj = null;
		String[] objStr = null;*/
		URI[] path;
		//static ArrayList<String> alist = new ArrayList<String>();
				@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			URI[] path = context.getCacheFiles();
			properties = new Properties();
			InputStream stream = null;
			super.setup(context);
			File file = new File(path[0]);
			//File file1 = new File(path[1]);
			stream = new FileInputStream(file);
			properties.load(stream);			
			stream.close();
			/*BufferedReader br = new BufferedReader(new FileReader(file1));
			String linecont;
			while((linecont = br.readLine())!=null){
				alist.add(linecont);
			}
			br.close();
			//System.out.println(alist.toString());*/
			alist = properties.keySet();
			//mos = new MultipleOutputs<Text, IntWritable>(context);
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		protected void map(LongWritable key, Text value, Mapper.Context context)
				throws IOException, InterruptedException {
			String document = value.toString();		
			document = document.replace("&#x26;", "dsba");
			Boolean flag = false;
			/*Boolean flagBofa = false;
			Boolean flagMicrosoft = false;
			Boolean flagTwc = false;
			Boolean flagGoldman = false;
			Boolean flagAtt = false;*/
			//String org = "";
			try {
				XMLStreamReader reader = XMLInputFactory.newInstance()
						.createXMLStreamReader(
								new ByteArrayInputStream(document.getBytes()));
				String currentElement = "";
				String propertyName = "";
				//Pattern REPLACE = Pattern.compile("[][.,;?!#$%<>/(){}]]");
				//("(?i).*"+temp+".*")){
				while (reader.hasNext()) {
					int code = reader.next();
					switch (code) {
					case 1:
						currentElement = reader.getLocalName();
						break;
					case 4:
						if (currentElement.equalsIgnoreCase("assignees")) {
							flag = true;
						}else if(currentElement.equalsIgnoreCase("orgname") && flag==true){
							propertyName = reader.getText();
							propertyName = propertyName.trim();
							propertyName = propertyName.toLowerCase();
							if(propertyName.contains("dsba")){
								propertyName = propertyName.replace("dsba", "&");
							}
							//propertyName = REPLACE.matcher(propertyName).replaceAll("");
							Iterator iterator = alist.iterator();
							while(iterator.hasNext()){
								String temp = (String)iterator.next();
								temp = temp.trim();
								if(propertyName.matches(temp)){
								this.word.set(properties.getProperty(temp));
								context.write(this.word, one);
								}
								
							}
							/*if(propertyName.contains("bank of america")){
								flagBofa = true;
							}else if(propertyName.contains("microsoft")){
								flagMicrosoft = true;
							}else if(propertyName.contains("time warner")){
								flagTwc = true;
							}else if(propertyName.contains("at&t")){
								flagAtt = true;
							}else if(propertyName.contains("goldman")){
								flagGoldman = true;
							}*/
							flag = false;
						}
						/*else if(currentElement.equalsIgnoreCase("claim-text") && flagBofa){
							mos.write("bofaclaim",new Text(reader.getText()) , NullWritable.get());
						}else if(currentElement.equalsIgnoreCase("claim-text") && flagMicrosoft){
							mos.write("microsoftclaim",new Text(reader.getText()) ,  NullWritable.get());
						}else if(currentElement.equalsIgnoreCase("claim-text") && flagTwc){
							mos.write("timewarnerclaim",new Text(reader.getText()) ,  NullWritable.get());
						}else if(currentElement.equalsIgnoreCase("claim-text") && flagGoldman){
							mos.write("goldmanclaim",new Text(reader.getText()) ,  NullWritable.get());
						}else if(currentElement.equalsIgnoreCase("claim-text") && flagAtt){
							mos.write("att",new Text(reader.getText()) ,  NullWritable.get());
						}
						
						else if(currentElement.equalsIgnoreCase("abstract") && flagBofa){
							mos.write("bofaabs",new Text(reader.getText()) , NullWritable.get());
						}else if(currentElement.equalsIgnoreCase("abstract") && flagMicrosoft){
							mos.write("microsoftabs",new Text(reader.getText()) ,  NullWritable.get());
						}else if(currentElement.equalsIgnoreCase("abstract") && flagGoldman){
							mos.write("goldmanabs",new Text(reader.getText()) ,  NullWritable.get());
						}else if(currentElement.equalsIgnoreCase("abstract") && flagTwc){
							mos.write("timeabs",new Text(reader.getText()) ,  NullWritable.get());
						}else if(currentElement.equalsIgnoreCase("abstract") && flagAtt){
							mos.write("attabs",new Text(reader.getText()) ,  NullWritable.get());
						}*/
						break;
					}	
					
				}
				reader.close();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			//mos.close();				
		}
		
	}
	//Reducer begins //
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			//Same as word count
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		//Define a new configuration file
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<us-patent-grant");
		conf.set("xmlinput.end", "</us-patent-grant>");
		// Keep properties file in classpath when executing in eclipse
		// Keep properties file in local path when executing in HDFS
		File file = new File("fortuneData.properties");
		//File file1 = new File("firstword.txt");
		/*BufferedReader inputStream = new BufferedReader(new FileReader(file1));
		String linecontent;
		ArrayList<String> al = new ArrayList<String>();
		while ((linecontent = inputStream.readLine()) != null) {
			String[] tempVal = linecontent.split("=");
			al.add(tempVal[0]);
		}
		String[] als = new String[al.size()];
		als = (String[]) al.toArray(als);
		conf.setStrings("propetries", als);
		inputStream.close();*/
		Job job = Job.getInstance(conf);
		//Distributed Cache 
		job.addCacheFile(file.toURI());
		//job.addCacheFile(file1.toURI());
		job.setJarByClass(XMLParsing.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(XmlInputFormat1.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
/*
 		MultipleOutputs.addNamedOutput(job, "bofaclaim", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "microsoftclaim", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "timewarnerclaim", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "goldmanclaim", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "att", TextOutputFormat.class, Text.class, Text.class);
		
		MultipleOutputs.addNamedOutput(job, "bofaabs", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "microsoftabs", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "goldmanabs", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "timeabs", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "attabs", TextOutputFormat.class, Text.class, Text.class);
*/
			job.waitForCompletion(true);
	}
}