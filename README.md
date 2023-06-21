# FlinkIOT
Using Flink with Azure IOT Hub

###Purpose:
> The purpose of this project is to connect Flink to Azure IoT Hub to gather telemetry data using a tempature sensor.
> The Tempature sensor will send dummy data in the form of a Json.  Once the tempature sends the data Flink will get the message
> and transform the data.  After transforming the data it will save to HDFS(ADLS Gen 2 Storage).

###Content
[**Purpose**][Purpose]<br>
[**Configuring The Azure IoT Device**](#Configuring-The-Azure-IoT-Device)<br>


###Configuring Azure IoT

To start setting up your Azure IoT you can follow this link

```https://learn.microsoft.com/en-us/azure/iot-develop/quickstart-send-telemetry-iot-hub?pivots=programming-language-java```

If you prefer to follow along here you can do so as well.

The first thing that is needed is downloading JDK.  Here we will be using 



























[Purpose]: #Purpose
[Configuring The Azure IoT Device]: #Configuring-The-Azure-IoT-Device
