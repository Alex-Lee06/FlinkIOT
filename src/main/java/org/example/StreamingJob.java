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

package org.example;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

		private static final String FILE_PATH = "src/main/resources/application.properties";

		public static void main(String[] args){
			try {
				//Load properties from config file
				//Properties properties = new Properties();
				//properties.load(new FileReader(FILE_PATH));
				StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

				//String Topic = properties.getProperty("EventHubName");
				//String broker = properties.getProperty("bootstrapServers");
				//String consumerGroup = properties.getProperty("consumerGroupID");

				final HashSet<TopicPartition> partitionSet = new HashSet<TopicPartition>(Arrays.asList(
						new TopicPartition("iothub-ehub-telemetryd-25096833-a002d81f0c", 0),
						new TopicPartition("iothub-ehub-telemetryd-25096833-a002d81f0c", 1)
				));

				String password = "Endpoint=sb://ihsuprodblres094dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=tOKQsqHOlyuv1xDZKCgTkxNjcoiLX58Xcadp85jri+k=;EntityPath=iothub-ehub-telemetryd-25096833-a002d81f0c";
				String connectionString= "HostName=telemetrydocker.azure-devices.net;DeviceId=myDevice;SharedAccessKey=e7DCUMzA1iESOyEnfwgjZOWSVUTNnEmHxU/KyQymVvI=";
				KafkaSource<String> source = KafkaSource.<String>builder()
						//.setBootstrapServers("137.135.102.226:9093")
						.setBootstrapServers("ihsuprodblres094dednamespace.servicebus.windows.net:9093")
						.setTopics("iothub-ehub-telemetryd-25096833-a002d81f0c")
						//.setPartitions(partitionSet)
						.setGroupId("$Default")
						.setProperty("partition.discovery.interval.ms", "10000")
						.setClientIdPrefix("iotflinkmessages")
						//.setProperty("security.protocol", "SASL_SSL")
						//.setProperty("sasl.mechanism", "SCRAM-SHA-256")
						.setProperty("security.protocol", "SASL_PLAINTEXT")
						.setProperty("sasl.mechanism", "PLAIN")
						//.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + connectionString + "\" password=\"" + password + "\";")
						.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"HostName=telemetrydocker.azure-devices.net;DeviceId=myDevice;SharedAccessKey=e7DCUMzA1iESOyEnfwgjZOWSVUTNnEmHxU/KyQymVvI=\" password=\"Endpoint=sb://flinkev.servicebus.windows.net/;SharedAccessKeyName=iothubroutes_telemetrydocker;SharedAccessKey=NSAjLJPAl9gyDIB8LsKYfyCs36G1bXZDn+AEhPCsTdw=;EntityPath=iotevflink\";")
						.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
						.setValueOnlyDeserializer(new SimpleStringSchema())
						.build();

				DataStream<String> kafka = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
				kafka.print();

				String outputPath  = "abfs://iotmessages@gen2alleestorage.dfs.core.windows.net/messagesfromiot/";

				final FileSink<String> sink = FileSink
						.forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
						.withRollingPolicy(
								DefaultRollingPolicy.builder()
										.withRolloverInterval(Duration.ofMinutes(2))
										.withInactivityInterval(Duration.ofMinutes(3))
										.withMaxPartSize(MemorySize.ofMebiBytes(5))
										.build())
						.build();

				kafka.sinkTo(sink);

				env.execute("IoT-Flink-Gen2 sink");
			} catch(FileNotFoundException e){
				System.out.println("FileNoteFoundException: " + e);
			} catch (Exception e){
				System.out.println("Failed with exception " + e);
			}
	}
}
