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


import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;
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

		public static void main(String[] args) {
			try {
				//Load properties from config file
				//Properties properties = new Properties();
				//properties.load(new FileReader(FILE_PATH));
				StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

				//String Topic = properties.getProperty("EventHubName");
				//String broker = properties.getProperty("bootstrapServers");
				//String consumerGroup = properties.getProperty("consumerGroupID");

				String password = "Endpoint=sb://ihsuprodblres094dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=tOKQsqHOlyuv1xDZKCgTkxNjcoiLX58Xcadp85jri+k=;EntityPath=iothub-ehub-telemetryd-25096833-a002d81f0c";
				String connectionString= "HostName=telemetrydocker.azure-devices.net;DeviceId=myDevice;SharedAccessKey=e7DCUMzA1iESOyEnfwgjZOWSVUTNnEmHxU/KyQymVvI=";
				KafkaSource<String> source = KafkaSource.<String>builder()
						.setBootstrapServers("ihsuprodblres094dednamespace.servicebus.windows.net:443")
						.setTopics("iothub-ehub-telemetryd-25096833-a002d81f0c")
						.setGroupId("iotflinkmessages")
						.setProperty("security.protocol", "SSL")
						.setProperty("sasl.mechanism", "PLAIN")
						.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + connectionString + "\" password=\"" + password + "\";")
						.setStartingOffsets(OffsetsInitializer.earliest())
						.setValueOnlyDeserializer(new SimpleStringSchema())
						.build();

				DataStream<String> kafka = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
				kafka.print();

				env.execute("Testing flink consumer");
			} catch(FileNotFoundException e){
				System.out.println("FileNoteFoundException: " + e);
			} catch (Exception e){
				System.out.println("Failed with exception " + e);
			}
	}
}
