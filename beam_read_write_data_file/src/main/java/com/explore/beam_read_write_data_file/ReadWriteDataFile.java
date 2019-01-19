package com.explore.beam_read_write_data_file;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
/*
 * Example on how to read and write data from/to a file
 */
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class ReadWriteDataFile {

	// Method which configures pipeline for flink runner
	public static PipelineOptions flinkPipelineOption(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(FlinkPipelineOptions.class);
		options.setRunner(FlinkRunner.class);
		return options;
	}

	// Read data from a file
	public static PCollection<String> readData(Pipeline pipeline, String path) {
		PCollection<String> data = pipeline.apply("ReadData", TextIO.read().from(path));
		return data;
	}

	// a method to write data to a file
	public static PDone writeData(PCollection<String> input, String outputPath) {
		PDone outputData = input.apply(TextIO.write().to(outputPath).withSuffix(".csv"));
		return outputData;
	}

	public static void main(String[] args) {

		// Create pipeline
		Pipeline pipeline = Pipeline.create(flinkPipelineOption(args));

		// Read data
		PCollection<String> input = readData(pipeline, "/D://Lucky/Dataset/input.txt");

		// Write data
		PCollection<String> words = input.apply(ParDo.of(new DoFn<String, String>() {
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

		PDone output = writeData(words, "/Lucky//dataset/output");

		State result = pipeline.run().waitUntilFinish();

		// Check pipeline status
		System.out.println(result);

	}
}
