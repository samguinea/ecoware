package ecoware.ecowareaccessmanager;


/**
 * @author Armando Varriale
 *
 */
public class ECoWareMessage {
	private String publisherID;
	private long timestamp;
	private Object messageBody;
	private ECoWareEventType eventType;
	private String eventName;
	private int eventId;
	
	/**
	 * Create an empty ECoWare message.
	 */
	public ECoWareMessage() {
		this.publisherID = null;
		this.timestamp = 0;
		this.messageBody = null;
		this.eventType = null;
		this.eventName = null;
		this.eventId = -1;
	}
	
	/**
	 * Create a ECoWare message for a specific standard event (eg "AvgRT", "ArrivalRate", etc.).
	 * @param publisherID the publisher ID
	 * @param messageBody the message body
	 * @param timestamp the message timestamp
	 * @param eventType the event type
	 * @param eventId the event id
	 */
	public ECoWareMessage(String publisherID, Object messageBody, long timestamp, ECoWareEventType eventType, int eventId) {
		this.publisherID = publisherID;
		this.timestamp = timestamp;
		this.messageBody = messageBody;
		this.eventType = eventType;
		this.eventName = eventType.getValue();
		this.eventId = eventId;
	}
	
	/**
	 * Create a ECoWare message for a custom event.
	 * @param publisherID the publisher ID
	 * @param messageBody the message body
	 * @param timestamp the message timestamp
	 * @param eventName the name for a custom event
	 * @param eventId the event id
	 */
	public ECoWareMessage(String publisherID, Object messageBody, long timestamp, String eventName, int eventId) {
		this.publisherID = publisherID;
		this.timestamp = timestamp;
		this.messageBody = messageBody;
		this.eventType = ECoWareEventType.CUSTOM_EVENT;
		this.eventName = eventName;
		this.eventId = eventId;
	}
	
	/**
	 * Returns the publisher ID.
	 * @return the publisherID
	 */
	public String getPublisherID() {
		return publisherID;
	}
	
	/**
	 * Sets the publisher ID.
	 * @param publisherID the publisherID to set
	 */
	public void setPublisherID(String publisherID) {
		this.publisherID = publisherID;
	}
	
	/**
	 * Returns the body content of the message.
	 * @return the messageBody
	 */
	public Object getMessageBody() {
		return messageBody;
	}
	
	/**
	 * Sets the body content of the message.
	 * @param messageBody the messageBody to set
	 */
	public void setMessageBody(String messageBody) {
		this.messageBody = messageBody;
	}

	/**
	 * Returns the timestamp of the message.
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * Sets the timestamp of the message.
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * Returns the event type associated to the message.
	 * @return the event type
	 */
	public ECoWareEventType getEventType() {
		return eventType;
	}

	/**
	 * Sets the event type associated to the message.
	 * @param eventType the event type to set
	 */
	public void setEventType(ECoWareEventType eventType) {
		this.eventType = eventType;
	}

	/**
	 * Returns the name of the event associated to the message.
	 * @return the event name
	 */
	public String getEventName() {
		return eventName;
	}

	/**
	 * Sets the name of the event associated to the message.
	 * @param eventName the event name to set
	 */
	public void setEventName(String eventName) {
		this.eventName = eventName;
	}

	/**
	 * Returns the id of the event associated to the message.
	 * @return the event id
	 */
	public int getEventId() {
		return eventId;
	}

	/**
	 * Sets the id of the event associated to the message.
	 * @param eventId the event id to set
	 */
	public void setEventId(int eventId) {
		this.eventId = eventId;
	}
}