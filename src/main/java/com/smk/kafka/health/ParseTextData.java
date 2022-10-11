package com.smk.kafka.health;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ParseTextData {

	public static void main(String[] args) {
		String filePath = "src/main/resources/txtfiles/HealthAuth-20220915.txt";

		try {
			BufferedReader lineReader = new BufferedReader(new FileReader(filePath));
			String lineText = null;
			
			ObjectNode jsonData = JsonNodeFactory.instance.objectNode();;

			while ((lineText = lineReader.readLine()) != null) {
				if (!(lineText.startsWith("HDR") || lineText.startsWith("TRL"))) {

					if (lineText.startsWith("SUB")) {
						jsonData = JsonNodeFactory.instance.objectNode();
						ObjectNode extractSubscriber = extractSubscriber(lineText);
						jsonData.put("Subscriber", extractSubscriber);
						System.out.println(extractSubscriber);
					} else if (lineText.startsWith("PAT")) {
						ObjectNode extractPatient = extractPatient(lineText);
						jsonData.put("Patient", extractPatient);
						System.out.println(extractPatient);

					} else if (lineText.startsWith("CAS")) {
						ObjectNode extractCase = extractCase(lineText);
						jsonData.put("Cases", extractCase);
						System.out.println(extractCase);

					} else if (lineText.startsWith("SVC")) {
						ObjectNode extractService = extractService(lineText);
						System.out.println(extractService);
						jsonData.put("Service",extractService);
						pushToKafkaTopic(jsonData);
						
						System.out.println(jsonData);

					}

//					String[] split = lineText.split("\\s{2,20}");
//					;
//					for (int i = 0; i < split.length; i++) {
//						System.out.println(split[i]);
//					}
//					System.out.println("\n");
				}
			}

			lineReader.close();
		} catch (IOException ex) {
			System.err.println(ex);
		}

	}

	private static void pushToKafkaTopic(ObjectNode jsonData) {
		Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // leverage idempotent producer from Kafka 0.11 !
        //properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        Producer<String, String> producer = new KafkaProducer<>(properties);
        
        producer.send(new ProducerRecord<>("Auth-Topic", jsonData.toString()));
        
        producer.close();

		
	}

	private static ObjectNode extractService(String lineText) {
		
//		1	Record Type (SVC)	1	3	N/A
//		2	Case Number	4	19	CASE_NUMBER
//		3	Service Number	20	35	SVC_ID
//		4	Service Type	36	51	SVC_TYPE
//		5	Serice Code	52	67	SVC_CODE
//		6	Faciity Id	68	83	SVC_FAC_ID
//		7	Facility Name	84	99	SVC_FAC_NAME
//		8	Physician Id	100	115	SVC_PHY_ID
//		9	Physician Name	116	131	SVC_PHY_NAME
		
		ObjectNode Service = JsonNodeFactory.instance.objectNode();
		Service.put("CASE_NUMBER",lineText.substring(3, 19).trim());
		Service.put("SVC_ID",lineText.substring(19, 35).trim());
		Service.put("SVC_TYPE",lineText.substring(35, 51).trim());
		Service.put("SVC_CODE",lineText.substring(51, 67).trim());
		Service.put("SVC_FAC_ID",lineText.substring(67, 83).trim());
		Service.put("SVC_FAC_NAME",lineText.substring(83, 99).trim());
		Service.put("SVC_PHY_ID",lineText.substring(99, 108).trim());
		Service.put("SVC_PHY_NAME",lineText.substring(109, lineText.length()).trim());
		
		return Service;
		
	}

	private static ObjectNode extractCase(String lineText) {
		
//		1	Record Type (CAS)	1	3	N/A
//		2	Case Number	4	19	CASE_NUMBER
//		3	Case Type	20	35	CASE_TYPE
//		4	Case Code	36	51	CASE_CODE
//		5	Case Start Date	52	67	CASE_START_DATE
//		6	Case End Date	68	83	CASE_END_DATE
//		7	Authorization Type	84	99	CASE_AUTH_TYPE
//		8	Case Status	100	115	CASE_STATUS
		
		ObjectNode Case = JsonNodeFactory.instance.objectNode();
		Case.put("CASE_NUMBER",lineText.substring(3, 19).trim());
		Case.put("CASE_TYPE",lineText.substring(19, 35).trim());
		Case.put("CASE_CODE",lineText.substring(35, 51).trim());
		Case.put("CASE_START_DATE",lineText.substring(51, 67).trim());
		Case.put("CASE_END_DATE",lineText.substring(67, 83).trim());
		Case.put("CASE_AUTH_TYPE",lineText.substring(83, 99).trim());
		Case.put("CASE_STATUS",lineText.substring(99, 108).trim());
		
		
		return Case;
		
	}

	private static ObjectNode extractPatient(String lineText) {
		
//		1	Record Type (PAT)	1	3	N/A
//		2	Case Number	4	19	CASE_NUMBER
//		3	Patient Number	20	35	PAT_ID
//		4	First Name	36	51	PAT_FIRST_NAME
//		5	Middle Name	52	67	PAT_MIDDLE_NAME
//		6	Last Name	68	83	PAT_LAST_NAME
//		7	Gender	84	99	PAT_SEX
//		8	Date OF Birth (YYYYMMDD)	100	115	PAT_DOB
//		9	Plan Type	116	131	PAT_PLANE_TYPE
//		10	Plan Name	132	147	PAT_PLAN_NAME
		
		ObjectNode Patient = JsonNodeFactory.instance.objectNode();
		Patient.put("CASE_NUMBER",lineText.substring(3, 19).trim());
		Patient.put("PAT_ID",lineText.substring(19, 35).trim());
		Patient.put("PAT_FIRST_NAME",lineText.substring(35, 51).trim());
		Patient.put("PAT_MIDDLE_NAME",lineText.substring(51, 67).trim());
		Patient.put("PAT_LAST_NAME",lineText.substring(67, 83).trim());
		Patient.put("PAT_SEX",lineText.substring(83, 99).trim());
		Patient.put("PAT_DOB",lineText.substring(99, 115).trim());
		Patient.put("PAT_PLANE_TYPE",lineText.substring(115, 131).trim());
		Patient.put("PAT_PLAN_NAME",lineText.substring(131, 133).trim());
		
		
		return Patient;
		
	}

	private static ObjectNode extractSubscriber(String lineText) {
//		1	Record Type (SUB)	1	3	N/A
//		2	Case Number	4	19	CASE_NUMBER
//		3	Subcriber Number	20	35	MEM_ID
//		4	First Name	36	51	MEM_FIRST_NAME
//		5	Middle Name	52	67	MEM_MIDDLE_NAME
//		6	Last Name	68	83	MEM_LAST_NAME
//		7	Address 1	84	99	MEM_ADD_1
//		8	Address 2	100	115	MEM_ADD_2
//		9	City	116	131	MEM_CITY
//		10	Zip Code	132	147	MEM_PIN
		
//		System.out.println(lineText.substring(3, 19));
//		System.out.println(lineText.substring(19, 35));
//		System.out.println(lineText.substring(35, 51));
//		System.out.println(lineText.substring(51, 67));
//		System.out.println(lineText.substring(67, 83));
//		System.out.println(lineText.substring(83, 99));
//		System.out.println(lineText.substring(99, 115));
//		System.out.println(lineText.substring(115, 131));
//		System.out.println(lineText.substring(132, 147));
		
		System.out.println("======================");
		
		ObjectNode Subscriber = JsonNodeFactory.instance.objectNode();
		Subscriber.put("CASE_NUMBER",lineText.substring(3, 19).trim());
		Subscriber.put("MEM_ID",lineText.substring(19, 35).trim());
		Subscriber.put("MEM_FIRST_NAME",lineText.substring(35, 51).trim());
		Subscriber.put("MEM_MIDDLE_NAME",lineText.substring(51, 67).trim());
		Subscriber.put("MEM_LAST_NAME",lineText.substring(67, 83).trim());
		Subscriber.put("MEM_ADD_1",lineText.substring(83, 99).trim());
		Subscriber.put("MEM_ADD_2",lineText.substring(99, 115).trim());
		Subscriber.put("MEM_CITY",lineText.substring(115, 131).trim());
		Subscriber.put("MEM_PIN",lineText.substring(131, 147).trim());
		
		return Subscriber;

	}

}
