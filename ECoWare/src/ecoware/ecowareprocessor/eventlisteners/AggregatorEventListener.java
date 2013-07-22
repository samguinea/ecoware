package ecoware.ecowareprocessor.eventlisteners;

import java.util.HashMap;
import java.util.Iterator;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import ecoware.ecowareaccessmanager.ECoWareEventType;
import ecoware.ecowareaccessmanager.ECoWareMessageSender;
import ecoware.util.*;

/**
 * 
 * @author Armando Varriale
 * 
 * This class is an implementation of UpdateListener interface provided by Esper and it's used for Aggregator Kpis.<br/> 
 * For further detail see the <a href="http://esper.codehaus.org/" target="_blank">Esper</a> documentation.
 *
 */
public class AggregatorEventListener implements UpdateListener {
	
	private String kpiName;
	private String kpiType;
	private String publicationID;
	private ECoWareMessageSender ecowareSender;
	private int eventId;
	
	/**
	 * Constructs a new AggregatorEventListener for the specified KPI name, which has the specified publication ID. 
	 * @param kpiName the KPI name
	 * @param publicationID the KPI publication ID
	 * @param busServer the hostname on which the bus server is running.
	 */
	public AggregatorEventListener(String kpiName, String publicationID, String busServer) {
		this(kpiName, publicationID, busServer, -1);
	}
	
	/**
	 * Constructs a new AggregatorEventListener for the specified KPI name, which has the specified publication ID and 
	 * the specified sequence number (named here "kpiId"). 
	 * @param kpiName the KPI name
	 * @param publicationID the KPI publication ID
	 * @param busServer the hostname on which the bus server is running.
	 */
	public AggregatorEventListener(String kpiName, String publicationID, String busServer, int kpiId) {
		
		this.kpiName = kpiName;
		this.kpiType = ECoWareEventType.getEventType(kpiName).getValue();
		this.publicationID = publicationID;
		this.eventId = kpiId;
		this.ecowareSender = new ECoWareMessageSender(busServer, this.publicationID);
	}

	@SuppressWarnings("unchecked")
	/**
	 * According to the UpdateListener interface, this is the update event by which the event listener 
	 * update the application/system state in reaction of the arrivals of new Esper events.<br/>
	 * For further detail see the <a href="http://esper.codehaus.org/" target="_blank">Esper</a> documentation.
	 */
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		EventBean tempEvent;
		HashMap<String, Object> eventMap, secondaryEventsMap, tempMap;
		Iterator<String> tempIterator;
		String tempAttributeName;
    	Object tempAttributeValue;
    	ECoWareEventType eventType = null;
    	
    	for(int i = 0; i < newEvents.length; i++) {
    		Logger.logDebug("-> For " + (i+1) + "/"+newEvents.length+" <-");
			tempEvent = newEvents[i];

		    eventMap = new HashMap<String, Object>(0);
		    eventMap.put("eventType", kpiType);
		    eventMap.put("eventName", kpiName);
		    eventMap.put("originID", publicationID);
		    
		    Logger.logInfo("**********************************************************");
		    Logger.logInfo("Event Type: " + kpiType);
		    Logger.logInfo("Event Name: " + kpiName);

			tempMap = (HashMap<String, Object>) tempEvent.getUnderlying();
			tempIterator = tempMap.keySet().iterator();
			secondaryEventsMap = new HashMap<String, Object>(0);
			eventMap.put("SecondaryEventsMap", secondaryEventsMap);
			while(tempIterator.hasNext()) {
				tempAttributeName = tempIterator.next();
				tempAttributeValue = tempMap.get(tempAttributeName);
				Logger.logInfo("Att.Name: " + tempAttributeName + "  Att.Value: " + tempAttributeValue);
				if(tempAttributeName.contains("PrimaryEvent")){
					if(!tempAttributeName.contains("timestamp")) 
						eventMap.put(tempAttributeName, tempAttributeValue);
					else
						eventMap.put("timestamp", tempAttributeValue);
				}
				else{
					secondaryEventsMap.put(tempAttributeName, tempAttributeValue);
				}
			}
			Logger.logInfo("**********************************************************");
		    try {
		    	eventType = ECoWareEventType.getEventType(this.kpiType);
				ecowareSender.startConnection();
				ecowareSender.send(eventMap, eventType, this.kpiName, eventId);
				ecowareSender.stopConnection();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}