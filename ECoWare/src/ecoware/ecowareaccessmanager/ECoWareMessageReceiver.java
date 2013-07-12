package ecoware.ecowareaccessmanager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;


/**
 * @author Armando Varriale
 * <br/><br/>
 * This  class represent a ECoWare receiver that is an entity able to connect to the (ECoWare) message bus, 
 * subscribe to one or more specific queues (that are related to messages that it wants receive) and then listen 
 * for the arrive of these messages. 
 */
public class ECoWareMessageReceiver {
	private String host;
	private ArrayList<String> subscribeKeys;
	private final String exchangeType = "direct";
	private final String exchangeName = "ecoware";
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	private QueueingConsumer consumer; 
	private String queueName;
	private ECoWareMessage msg;
	private ReceiverThread rt;
	private Thread t;
	private ArrayList<ECoWareMessageListener> listeners;
	
	/**
	 * Constructs a new ECoWareMessageReceiver with the specified host name.
	 * @param host the host on which the bus is running
	 */
	public ECoWareMessageReceiver(String host) {
		this(host, null);
	}
		
	/**
	 * Constructs a new ECoWareMessageReceiver with the specified host name and subscribe keys.
	 * @param host the host on which the bus is running
	 * @param subscribeKeys subscribe keys (o IDs)
	 */
	public ECoWareMessageReceiver(String host, ArrayList<String> subscribeKeys) {
		this.host = host;
		if(subscribeKeys == null)
			this.subscribeKeys = null;
		else
			this.subscribeKeys = new ArrayList<String>(subscribeKeys);
		this.listeners = new ArrayList<ECoWareMessageListener>();
	}

	// Methods
	/**
	 * Start the connection to the bus.
	 * @throws IOException
	 * @throws Exception 
	 */
	public void startConnection() throws IOException, Exception{
		createConnection();
		this.queueName = routeSetup();
		createConsume(this.queueName);
		rt = new ReceiverThread(this);
		t = new Thread(rt);
		t.start();
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
	 * @throws InterruptedException 
	 */
	public void stopConnection() throws IOException, InterruptedException {
		this.rt.stopRunning();
		this.t.join();
		Thread.sleep(1000);
		// Close channel
		this.channel.close();
		this.connection.close();
	}
	
	/**
	 * Create the route setup for the routing
	 * @return the queue name that has been created
	 * @throws IOException
	 * @throws Exception
	 */
	private String routeSetup() throws IOException, Exception {
		if(!channel.isOpen() || channel==null) throw new Exception("Error: the connection has not been opened or created.");
			
		// channel declaration
		if(exchangeName==null) throw new Exception("Error: the name of the exchange is not defined.");
		
		channel.exchangeDeclare(exchangeName, exchangeType);
		
		// queue creation and binding
		String queueName = channel.queueDeclare().getQueue();
		if(!subscribeKeys.isEmpty())
		{
			for(String key: subscribeKeys)
			{
				channel.queueBind(queueName, exchangeName, key);
			}
		}
		return queueName;
	}
	
	/**
	 * Create and start a consumer process 
	 * @param queueName the name of the queue where store messages
	 * @throws IOException
	 * @throws Exception
	 */
	private void createConsume(String queueName) throws IOException, Exception{
		if(!channel.isOpen() || channel==null) throw new Exception("Error: the connection has not been opened or created.");
		consumer = new QueueingConsumer(channel);
		channel.basicConsume(queueName, true, consumer);
	}
	
	/**
	 * Receive a message from the bus
	 * @return a ECoWareMessage variable
	 * @throws ShutdownSignalException
	 * @throws ConsumerCancelledException
	 * @throws InterruptedException
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private ECoWareMessage receive() throws ShutdownSignalException, ConsumerCancelledException, InterruptedException, Exception{
		if(consumer==null) throw new Exception("Error: the consumer process not been created.");
		ECoWareMessage msg;
		QueueingConsumer.Delivery delivery = consumer.nextDelivery();
		String publisherId = delivery.getProperties().getMessageId();
		HashMap<String, Object> hdr = (HashMap<String, Object>) delivery.getProperties().getHeaders();
		long timestamp = ((Number) hdr.get("timestamp")).longValue();
		ByteArrayInputStream byteIn = new ByteArrayInputStream(delivery.getBody());
		ObjectInputStream in = new ObjectInputStream(byteIn);	
		Map<String, Object> messageBody = (Map<String, Object>) in.readObject();
		int eventId = -1;
		
		if(ECoWareEventType.getEventType(hdr.get("eventType").toString()) != ECoWareEventType.CUSTOM_EVENT){
			ECoWareEventType eventType = ECoWareEventType.getEventType(hdr.get("eventType").toString());
			if(eventType.equals(ECoWareEventType.SECONDAY_AGGREGATION)) eventId = (int) hdr.get("eventID");
			msg = new ECoWareMessage(publisherId, messageBody, timestamp, eventType, eventId);
		}
		else{
			String eventName = hdr.get("eventName").toString();
			eventId = (int) hdr.get("eventID");
			msg = new ECoWareMessage(publisherId, messageBody, timestamp, eventName, eventId);
		}
		return msg;
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
	 * Returns the list of subscribe keys. The list is an ArrayList of strings.
	 * @return the subscribe keys list
	 */
	public ArrayList<String> getSubscribeKeys() {
		return subscribeKeys;
	}

	/**
	 * Sets the list of subscribe keys. The list is an ArrayList of strings.
	 * @param subscribeKeys the subscribe keys list to set
	 */
	public void setSubscribeKeys(ArrayList<String> subscribeKeys) {
		if(subscribeKeys == null)
			this.subscribeKeys = null;
		else
			this.subscribeKeys = new ArrayList<String>(subscribeKeys);
	}
	
	/**
	 * Add an ECoWare message listener.
	 * @param listener the listener, which must be an implementation of <a href="ECoWareMessageListener.html">ECoWareMessageListener</a> interface.
	 * @see ECoWareMessageListener
	 */
	public void addECoWareMessageListener(ECoWareMessageListener listener){
		this.listeners.add(listener);
	}
	
	/**
	 * Remove an ECoWare message listener
	 * @param listener the listener to remove (that is an implementation of <a href="ECoWareMessageListener.html">ECoWareMessageListener</a> interface).
	 * @see ECoWareMessageListener
	 */
	public void removeECoWareMessageListener(ECoWareMessageListener listener){
		this.listeners.remove(listener);
	}
	
	private void fireNotify(ECoWareMessage msg){
		for (ECoWareMessageListener listener : this.listeners) {
			listener.notify(msg);
		}
	}
	
	private class ReceiverThread implements Runnable{
		private ECoWareMessageReceiver ric;
		private boolean stop = false;
		
		public ReceiverThread(ECoWareMessageReceiver rc){
			ric = rc;
		}

		public void run() {
			while(!stop){
				try {
					ric.msg = ric.receive();
					if(ric.msg!=null) fireNotify(ric.msg);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		public void stopRunning()
		{
			stop = true;
		}
	}
}