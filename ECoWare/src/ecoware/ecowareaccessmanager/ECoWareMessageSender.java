package ecoware.ecowareaccessmanager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.HashMap;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Armando Varriale
 * <br/><br/>
 * This  class represent a ECoWare sender that is an entity able to connect to the (ECoWare) message bus and 
 * to publish (send) messages on the bus. The sender has a pubblication ID and thus the submitted messages have 
 * this publication ID, which is used by receivers (<a href="ECoWareMessageReceiver.html">ECoWareMessageReceiver</a>) to retrieve specific needed messages. 
 *
 */
public class ECoWareMessageSender {
	
	
	private String host;
	private String publishingKey;
	private final String exchangeType = "direct";
	private final String exchangeName = "ecoware";
	
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	
	private BasicProperties prop;
	
	/**
	 * Constructs a new ECoWareMessageSender with the specified host name.
	 * @param host the host on which the bus is running
	 */
	public ECoWareMessageSender(String host) {
		this(host, null);
	}
		
	/**
	 * Constructs a new ECoWareMessageSender with the specified host name and publishing key (ID).
	 * @param host the host on which the bus is running
	 * @param publishingKey the publishing Key (ID)
	 */
	public ECoWareMessageSender(String host, String publishingKey) {
		this.host = host;
		this.publishingKey = publishingKey;
	}

	// Methods
	/**
	 * Start the connection to the bus.
	 * @throws IOException
	 * @throws Exception 
	 */
	public void startConnection() throws IOException, Exception{
		createConnection();
		routeSetup();
	}
	
	/**
	 * Create the connection to the bus.
	 * @throws IOException
	 */
	private void createConnection() throws IOException {
		// Create connection elements
		this.factory = new ConnectionFactory();
		this.factory.setHost(this.host);
		this.connection = factory.newConnection();
		this.channel = connection.createChannel();	
	}
		
	/**
	 * Stop the connection to the bus.
	 * @throws IOException
	 */
	public void stopConnection() throws IOException {
		// Close channel
		this.channel.close();
		this.connection.close();
	}
	
	/**
	 * Create a new channel inside the connection whereby send messages.
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private void createNewChannel() throws IOException
	{
		channel = connection.createChannel();
	}
	
	/**
	 * Create a new channel inside the connection whereby send messages.
	 * @return the new created channel
	 * @throws IOException
	 */
	public Channel getNewChannel() throws IOException
	{
		return connection.createChannel();
	}
	
	/**
	 * Create the route setup for the routing.
	 * @throws IOException
	 * @throws Exception
	 */
	private void routeSetup() throws IOException, Exception {
		if(channel==null || !channel.isOpen()) throw new Exception("Error: the connection has not been opened or created.");
			
		// channel declaration
		if(exchangeName==null) throw new Exception("Error: the name of the exchange is not defined.");
		
		channel.exchangeDeclare(exchangeName, exchangeType);
	}
	
	/**
	 * Publish a message for a specific subscriber onto the bus.
	 * @param message the message to send
	 * @throws IOException
	 * @throws Exception
	 */
	public void send(String message) throws IOException, Exception{	
		if(!this.channel.isOpen() || this.channel==null) throw new Exception("Error: the connection has not been opened or created.");
		
		if(this.publishingKey!=null)
		{
			HashMap<String, Object> msgHeader = new HashMap<String, Object>();
			msgHeader.put("pubID", publishingKey);
			msgHeader.put("timestamp", System.currentTimeMillis());
			
			prop = new BasicProperties.Builder().messageId(publishingKey).timestamp(new Date()).headers(msgHeader).build();
			
			HashMap<String, Object> msgMap = new HashMap<String, Object>();
			msgMap.put(publishingKey, message);
			
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(byteOut);
			out.writeObject(msgMap);
			this.channel.basicPublish(exchangeName, publishingKey, prop, byteOut.toByteArray());
		}		
	}
	
	/**
	 * Publish a message related to a custom event onto the bus.
	 * @param message the message to send
	 * @param eventType the event type
	 * @param eventName the event name for a custom event
	 * @param eventId the ID of the custom event
	 * @throws IOException
	 * @throws Exception
	 */
	public void send(HashMap<String, Object> message, ECoWareEventType eventType, String eventName, int eventId) throws IOException, Exception{	
		if(this.channel==null || !this.channel.isOpen()) throw new Exception("Error: the connection has not been opened or created.");
		
		if(this.publishingKey!=null)
		{		
			HashMap<String, Object> msgHeader = new HashMap<String, Object>();
			msgHeader.put("pubID", publishingKey);
			msgHeader.put("timestamp", System.currentTimeMillis());
			msgHeader.put("eventType", eventType.getValue());
			msgHeader.put("eventName", eventName);
			
			if(eventType.equals(ECoWareEventType.SECONDAY_AGGREGATION) ||
			   eventType.equals(ECoWareEventType.CUSTOM_EVENT)) msgHeader.put("eventID", eventId);
			
			prop = new BasicProperties.Builder().messageId(publishingKey).timestamp(new Date()).headers(msgHeader).build();
					
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(byteOut);
			out.writeObject(message);
			this.channel.basicPublish(exchangeName, publishingKey, prop, byteOut.toByteArray());
		}		
	}
	
	/**
	 * Publish a message for a specific standard event onto the bus.
	 * @param message the message to send
	 * @param eventType the event type
	 * @param eventId the ID of a secondary aggregation or a custom event type (N.B. it will be considered only if the evenType is either 'SECONDARY AGGREGATION' or 'CUSTOM EVENT'.)
	 * @throws IOException
	 * @throws Exception
	 */
	public void send(HashMap<String, Object> message, ECoWareEventType eventType, int eventId) throws IOException, Exception{	
		if(this.channel==null || !this.channel.isOpen()) throw new Exception("Error: the connection has not been opened or created.");
		
		if(this.publishingKey!=null)
		{		
			HashMap<String, Object> msgHeader = new HashMap<String, Object>();
			msgHeader.put("pubID", publishingKey);
			msgHeader.put("timestamp", System.currentTimeMillis());
			msgHeader.put("eventType", eventType.getValue());
			msgHeader.put("eventName", eventType.getValue());
			if(eventType.equals(ECoWareEventType.SECONDAY_AGGREGATION) ||
			   eventType.equals(ECoWareEventType.CUSTOM_EVENT)) msgHeader.put("eventID", eventId);
			
			prop = new BasicProperties.Builder().messageId(publishingKey).timestamp(new Date()).headers(msgHeader).build();
					
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(byteOut);
			out.writeObject(message);
			this.channel.basicPublish(exchangeName, publishingKey, prop, byteOut.toByteArray());
		}		
	}
	
	/**
	 * Publish a message for a specific standard event onto the bus.
	 * @param message the message to send
	 * @param eventType the event type
	 * @param eventId the ID of a secondary aggregation or a custom event type (N.B. it will be considered only if the evenType is either 'SECONDARY AGGREGATION' or 'CUSTOM EVENT'.)
	 * @param channel the used channel to send the message 
	 * @throws IOException
	 * @throws Exception
	 */
	public void send(HashMap<String, Object> message, ECoWareEventType eventType, int eventId, Channel channel) throws IOException, Exception{	
		if(channel==null || !channel.isOpen()) throw new Exception("Error: the connection has not been opened or created.");
		
		if(this.publishingKey!=null)
		{		
			HashMap<String, Object> msgHeader = new HashMap<String, Object>();
			msgHeader.put("pubID", publishingKey);
			msgHeader.put("timestamp", System.currentTimeMillis());
			msgHeader.put("eventType", eventType.getValue());
			msgHeader.put("eventName", eventType.getValue());
			if(eventType.equals(ECoWareEventType.SECONDAY_AGGREGATION) ||
			   eventType.equals(ECoWareEventType.CUSTOM_EVENT)) msgHeader.put("eventID", eventId);
			
			prop = new BasicProperties.Builder().messageId(publishingKey).timestamp(new Date()).headers(msgHeader).build();
					
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(byteOut);
			out.writeObject(message);
			channel.basicPublish(exchangeName, publishingKey, prop, byteOut.toByteArray());
		}		
	}
	
	// Setter and Getter methods
	/**
	 * Returns the host name.
	 * @return the host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Sets the host name.
	 * @param host the host to set
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * Returns the publishing key.
	 * @return the publishing key
	 */
	public String getPublishingKey() {
		return publishingKey;
	}

	/**
	 * Sets the publishing key.
	 * @param publishingKey the publishing key to set
	 */
	public void setPublishingKey(String publishingKey) {
		this.publishingKey = publishingKey;
		
	}

}