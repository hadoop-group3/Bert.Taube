import java.io.IOException;
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import stock.intro.SectorList_TotalCapCounter;
import stock.intro.SectorList_TotalCapCounter.StockCountReducer;
import stock.intro.SectorList_TotalCapCounter.StockSectorMapper;

public class Homework1 {
	public static class StockCountReducer extends Reducer<Text, Text, Text, Text> {

		private final static Text TICKER_CAP = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			float dcap = 0;
			long lcap = 0;

			for (Text value : values) {
				String tokens[] = value.toString().split(",");
				TICKER_CAP.set(value);
				context.write(key, TICKER_CAP);
				// increment the capitalization counter
				dcap = Float.valueOf(tokens[1]) * 1000; // work in the millions
				lcap = (long) dcap;
				context.getCounter("SectorCount", key.toString() + " - total capitalization (millions)")
						.increment(lcap);

			}

		}
	}

	/*-
	 * Mapper for parsing sector info out of file, writing out the sector and ticker
	 *
	 * Filters out stocks with capitalization that is not in the billions
	 *
	 * Example record in the NASDAQ company list files:
	 *
	 * "GOOG","Google Inc.","523.4","$357.66B","2004","Technology","Computer Software","http://www.nasdaq.com/symbol/goog",
	*/
	public static class StockSectorMapper extends Mapper<Object, Text, Text, Text> {

		/*
		 * Create holders for output so we don't recreate on every map
		 */
		private final static Text TICKER_CAP = new Text();
		private final static Text KEY = new Text();

		/*-
		 * example of a data record in the NASDAQ company list files
		 * "GOOG","Google Inc.","523.4","$357.66B","2004","Technology","Computer Software","http://www.nasdaq.com/symbol/goog",
		*/
		private final static String recordRegex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
		private final static int sectorIndex = 5;
		private final static int capIndex = 3;
		private final static int tickerIndex = 0;
		public final static String NO_INFO = "n/a";

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] tokens = value.toString().split(recordRegex, -1);
			String sectorStr = tokens[sectorIndex].replace("\"", "");
			String tickerStr = tokens[tickerIndex].replace("\"", "");
			String capStr = tokens[capIndex].replace("\"", "");

			// skip the header
			if (sectorStr.equals("Sector"))
				return;

			if (tokens.length == 9) {

				if (!sectorStr.equals(NO_INFO) && capStr.endsWith("B")) {
					capStr = capStr.replace("B", "");
					capStr = capStr.replace("$", "");
					KEY.set(sectorStr);
					TICKER_CAP.set(tickerStr + "," + capStr);
					context.write(KEY, TICKER_CAP);
					context.getCounter("SectorCount", sectorStr + " - number of companies").increment(1);
				}
			}
		}

	}

	private static final Log LOG = LogFactory.getLog(SectorList_TotalCapCounter.class);

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new SectorList_TotalCapCounter(), args);
		System.out.println("Job completed with status:  " + exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			for (String arg : args)
				System.err.println(arg);
			System.err.println("Usage: sectorlist_capcounter <in> <out>");
			System.exit(0);
		}

		Configuration conf = getConf();
		LOG.info("input: " + args[0] + " output: " + args[1]);

		LOG.info("---- listing stocks by sector with counters for stocks per sector, total capitalization ------- ");

		Job job = Job.getInstance(conf,
				"listing stocks by sector with counters for stocks per sector, total capitalization");
		job.setJarByClass(SectorList_TotalCapCounter.class);
		job.setMapperClass(StockSectorMapper.class);
		job.setReducerClass(StockCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + Calendar.getInstance().getTimeInMillis()));

		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}
}
