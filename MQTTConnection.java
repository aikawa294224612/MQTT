package com.mqtt;

import java.sql.*;

import java.text.SimpleDateFormat;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;

public class MQTTConnection {
     
	private static String hostName="HOSTNAME";  
    public static String uuid="UUID";
	public static String token="TOKEN";	 
	private static String username= uuid ;
	private static String password= token ;
	private static String subscribeTopic= "12345" ;
	private static String publisheTopic= "message" ;
	private static String sendMsg="{\"devices\":\""+uuid+"\",\"payload\":{\"param1\":\"value1\"}}";
	private static MqttClient client ;
	
	public static void main(String[] args) {  
		subscribe();   //subscribe method 
//		publish();   //public method
	}
	  public static String subscribe() {   
	        try {   
	            //Create MqttClient  
	        	client=new MqttClient(hostName,"111");   
	            client.setCallback(new MqttCallback(){
					public void connectionLost(Throwable arg0) {
					}
					@Override
					public void deliveryComplete(IMqttDeliveryToken arg0) {
						// TODO Auto-generated method stub
						
					}
					@Override
					public void messageArrived(String topic, MqttMessage message) throws Exception {
						// TODO Auto-generated method stub
						 try {  
//							 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
//							 String date = sdf.format(new Date());
					            System.out.println(" From:"+message.toString()); 
					            
					            Connection conn = null;
					            Class.forName("com.mysql.jdbc.Driver"); 
					            conn = DriverManager.getConnection(
					            		"jdbc:mysql://localhost:3306/mqtt?useUnicode=true&characterEncoding=UTF-8",
					            		"root",
					            		"tmu2012");
					            System.out.println("connected to the database");

					            Statement stmt = conn.createStatement();
					            System.out.println("Inserting records");
					            
					            String qry1 = "INSERT INTO mymqtts (mqtt) VALUES('"+message.toString()+"')";
					            stmt.executeUpdate(qry1);
					            System.out.println("ok");
						 }catch (Exception e) {   
					            e.printStackTrace();  
					            System.out.println("error");  
					        } 
					}
	            	
	            });
	            MqttConnectOptions conOptions = new MqttConnectOptions();   
	            conOptions.setUserName(username);
	            conOptions.setPassword(password.toCharArray());
	            conOptions.setCleanSession(false);   
	            client.connect(conOptions);   
	            client.subscribe(subscribeTopic, 1); 
	            boolean isSuccess =client.isConnected();
	            System.out.println("status1:"+isSuccess);
	            //client.disconnect();   
	        } catch (Exception e) {   
	            e.printStackTrace(); 
	            System.out.println(" error");  
	            return "failed";   
	        }   
	        System.out.println("success");  
	        return "success";   
	    } 
	  
//	    public static void publish(){   
//	        try {   
//	            MqttTopic topic = client.getTopic(publisheTopic);   
//	            System.out.println("public:"+sendMsg);
//	            MqttMessage message = new MqttMessage(sendMsg.getBytes());   
//	            message.setQos(1);   
//	            while(true){  
//	                MqttDeliveryToken token = topic.publish(message); 
//	                while (!token.isComplete()){   
//	                    token.waitForCompletion(1000);   
//	                }   
//	            }  
//	        } catch (Exception e) {   
//	            e.printStackTrace();   
//	        }   
//	    }  
}