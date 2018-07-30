/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.itri.beam;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class StarterPipeline {

  public static void main(String[] args) {
	SparkPipelineOptions options = PipelineOptionsFactory.fromArgs(args).
	  withValidation().as(SparkPipelineOptions.class);    
	options.setRunner(SparkRunner.class);	
	Pipeline p = Pipeline.create(options); 
	options.setMaxRecordsPerBatch(1000L);	
	options.setSparkMaster("spark://ubuntu8:7077");
    
    PCollection<KV<Integer, String>> readData = p.apply(KafkaIO.<Integer, String>read()
       .withBootstrapServers("ubuntu7:9092")
       .withTopic("kafkasink")
       .withKeyDeserializer(IntegerDeserializer.class)
       .withValueDeserializer(StringDeserializer.class)       
       //.withMaxNumRecords(500000)
       .withoutMetadata());
    
    PCollection<KV<Integer, String>> readData1 = readData.
    apply(Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardSeconds(1)))
      .triggering(AfterWatermark.pastEndOfWindow()
        .withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.ZERO)))
      .withAllowedLateness(Duration.ZERO)
      .discardingFiredPanes());
    
    PCollection<KV<String, String>> readData2 = readData1.apply(ParDo.of(new DoFn<KV<Integer, String>,KV<String, String>>(){
	  @ProcessElement
	    public void test(ProcessContext c){		
		  System.out.println("data in window:" + c.element());
			c.output(KV.of("M1",c.element().getValue()));
		}
	  }));
   
    
    PCollection<KV<String, Long>> countData =readData2.apply(Count.perKey());
    
    countData.apply(JdbcIO.<KV<String, Long>>write()
      .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
        "org.postgresql.Driver",
        "jdbc:postgresql://ubuntu7:5432/raw_c42a25f4bd3d74429dbeb6162e60e5c7")
          .withUsername("postgres")
          .withPassword("postgres"))
       .withStatement("insert into kafkabeamdata (count) values(?)")
       .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<String, Long>>() {
          @Override
          public void setParameters(KV<String, Long> element, PreparedStatement query)
            throws SQLException {
        	  double count = element.getValue().doubleValue();
        	  query.setDouble(1, count);
        	}
       	  }));

    p.run().waitUntilFinish();    

  }
}