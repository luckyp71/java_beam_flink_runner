package com.explore.beam_read_write_data_file;

import org.apache.hadoop.conf.Configuration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
 * Example on how to read and write data from/to a file from hdfs
 */

public class ReadWriteDataFile {

	// A method which configures pipeline for accessing hdfs and using apache flink runner
	public static PipelineOptions pipelineOption(String[] args) {
		HadoopFileSystemOptions options = PipelineOptionsFactory.fromArgs(args).as(FlinkPipelineOptions.class)
				.as(HadoopFileSystemOptions.class);
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		List<Configuration> list = new ArrayList<Configuration>();
		list.add(conf);
		options.setRunner(FlinkRunner.class);
		options.setHdfsConfiguration(list);

		return options;
	}

	// Read data from a file
	public static PCollection<String> readData(Pipeline pipeline, String path) {
		PCollection<String> data = pipeline.apply("ReadData", TextIO.read().from(path));

		PCollection<String> words = data.apply(ParDo.of(new DoFn<String, String>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				String line = c.element();
				for (String word : line.split("[^a-z0-9A-Z']+")) {
					c.output(word);
					System.out.println(word);
				}
			}
		}));

		return words;
	}

	// A method to write data to a file
	public static PDone writeData(PCollection<String> input, String outputPath) {
		PDone outputData = input.apply(TextIO.write().to(outputPath).withSuffix(".csv"));
		return outputData;
	}

	public static void main(String[] args) throws IOException {

		// Creating pipeline
		Pipeline pipeline = Pipeline.create(pipelineOption(args));

		/*
		 * Read and process data -> change file path into yours, data must use comma
		 * separated like the data in the src/main/resource/input.txt (just for
		 * example).
		 */
		PCollection<String> data = readData(pipeline, "hdfs://localhost:9000/data/input.txt");

		/*
		 * Write data -> change file path and name into yours, the path example below
		 * will produce file called output.csv in /Lucky//dataset/output folder.
		 */
		PDone output = writeData(data, "hdfs://localhost:9000/data/output");

		State result = pipeline.run().waitUntilFinish();

		// Check pipeline status
		System.out.println(result);
	}
}