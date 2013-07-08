package ecoware.ecowareprocessor.kpi.aggregators;

import ecoware.ecowareprocessor.eventlisteners.KPIEventListener;
import ecoware.ecowareprocessor.kpi.KPIManager;
import ecoware.ecowareaccessmanager.ECoWareEventType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

/**
 * 
 * @author Armando Varriale
 * <br/><br/>
 * This class is an implementation of an "Aggregator KPI". It extends the <a href="KPIManager.html">KPIManager</a> 
 * abstract class by adding some specific aspects and provide an adequate implementation of the "launch()" method.
 * 
 * An "Aggregator" calculator is a processor that aggregates one or more events among them, correlating a set 
 * of secondary events to a specific primary event.
 * 
 * or a more detailed presentation of these concepts, see the provided <a href="">tutorials </a>section of the ECoWare documentation.
 *
 */
public class Aggregator extends KPIManager {
	
	private String aggregatorName;
	private String primaryEventName;
	private String secondaryEventName;
	private NodeList secondaryEvents;
	private Map<String, Object> secondaryEventsList;
	private ArrayList<String> eventSpec;
	private Element eventNode;
	private final boolean DEBUG_MODE = false;
	
	/**
	 * Constructs a new Aggregator using the specified XML element. The bus server name (that is 
	 * the hostname on which the bus server is running) and Esper configuration are also required.
	 * @param xmlElement the XML element (node) of the configuration file from which retrieve the information to build the KPI object
	 * @param busServer the hostname on which the bus server is running
	 * @param esperConfiguration the Esper current configuration (that is an Configuration object. For further detail see the <a href="http://esper.codehaus.org/" target="_blank">Esper</a> documentation).
	 */
	public Aggregator(Element xmlElement, String busServer, Configuration esperConfiguration) throws Exception {

		super(xmlElement, busServer, esperConfiguration);
		
		aggregatorName = xmlElement.getElementsByTagName("name").item(0).getTextContent();
		primaryEventName = xmlElement.getElementsByTagName("primaryEventName").item(0).getTextContent();
		
		secondaryEvents = xmlElement.getElementsByTagName("secondaryEvent");
		if(secondaryEvents.getLength()<1) throw new Exception("For an aggregator event at least one secondary event must be declared!");
		
		secondaryEventsList = new LinkedHashMap<String, Object>(0);
		for(int i=0; i<secondaryEvents.getLength(); i++){
			eventSpec =  new ArrayList<String>(0);
			eventNode = (Element) secondaryEvents.item(i);
			secondaryEventName = eventNode.getElementsByTagName("secondaryEventName").item(0).getTextContent();
			eventSpec.add(eventNode.getElementsByTagName("subscriptID").item(0).getTextContent());
			eventSpec.add(eventNode.getElementsByTagName("intervalUnit").item(0).getTextContent());
			eventSpec.add(eventNode.getElementsByTagName("intervalValue").item(0).getTextContent());
			eventSpec.add(secondaryEventName);
			secondaryEventsList.put(secondaryEventName, eventSpec);
		}
//		secondaryEventName = xmlElement.getElementsByTagName("secondaryEventName").item(0).getTextContent();
//
//		Element secondaryEvent = (Element) xmlElement.getElementsByTagName("secondaryEvent").item(0);
//		
//		secondaryEventIntervalUnit = secondaryEvent.getElementsByTagName("intervalUnit").item(0).getTextContent();
//		secondaryEventIntervalValue = Integer.parseInt(secondaryEvent.getElementsByTagName("intervalValue").item(0).getTextContent());
	}

	@SuppressWarnings("unchecked")
	@Override
	/**
	 * This method actually starts the "Aggregator" KPI processing.<br/>
	 */
	public void launch() {

		System.out.println("---");
		System.out.println("Initializing aggregator " + aggregatorName + "...");
		
		if(DEBUG_MODE){
			System.out.println("PubID = " + getPublicationID());
			int i=0;
			for(String subid: getSubscriptionIDs()){
				System.out.println("SubId" + i + ": " + subid);
				i++;
			}

			System.out.println("PrimaryEvent: " + primaryEventName);
			System.out.println("PrimaryEvent SubscriptID: " + getSubscriptionIDs().get(0));

			System.out.println("Numero secondary events: " + secondaryEvents.getLength());
			System.out.println("Numero secondary events: " + secondaryEventsList.size());

			Set<String> info = secondaryEventsList.keySet();
			ArrayList<String> tmpList;
			for(String secondEvent: info){
				System.out.println("Evento: " + secondEvent);
				tmpList = (ArrayList<String>)secondaryEventsList.get(secondEvent);
				for(String tmp: tmpList){
					System.out.println("-> " + tmp);
				}
			}
		}
		
        //ESPER configuration
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(getEsperConfiguration());

        //map for infrastructure events
		Set<String> secondEvents = secondaryEventsList.keySet();
		for(String secondEvent: secondEvents){
			if(!epService.getEPAdministrator().getConfiguration().isEventTypeExists(secondEvent)) {
				// secondary event map
				Map<String, Object> secondaryEventMap = new HashMap<String, Object>();
				secondaryEventMap.put("originID", String.class);
				epService.getEPAdministrator().getConfiguration().addEventType(secondEvent, secondaryEventMap);
			}
		}
		
		//EPL creation
		for(String secondEvent: secondEvents){ // Non necessario perchè integro "nome evento + pub_id" nel EventName al posto del semplice copia di EventType
			if(secondEvent.equals(primaryEventName))
				((ArrayList<String>)secondaryEventsList.get(secondEvent)).set(((ArrayList<String>)secondaryEventsList.get(secondEvent)).size() - 1, "Correlated_"+secondEvent);
		}

		// ESPER statement generation
		String esperStatement = "SELECT * FROM " + primaryEventName + " AS " + primaryEventName + " UNIDIRECTIONAL";
		//ciclo su i veri secondary_events
		ArrayList<String> secondEventSpecs;
		for(String secondEvent: secondEvents){
			secondEventSpecs = (ArrayList<String>)secondaryEventsList.get(secondEvent);
			esperStatement += ", " + secondEvent + ".win:time(" + secondEventSpecs.get(1) + " " + secondEventSpecs.get(2) + ") AS " + secondEventSpecs.get(3);
		}
		
		esperStatement += " WHERE " + primaryEventName + ".originID = '" + getSubscriptionIDs().get(0) + "'";
		// altro ciclo
		for(String secondEvent: secondEvents){
			secondEventSpecs = (ArrayList<String>)secondaryEventsList.get(secondEvent);
			esperStatement += " AND " + secondEventSpecs.get(3) + ".originID = '" + secondEventSpecs.get(0) + "'";
		}
		
		System.out.println("Query: " + esperStatement);
		EPStatement eplStatement = epService.getEPAdministrator().createEPL(esperStatement);
		
		eplStatement.addListener(new KPIEventListener(ECoWareEventType.AGGREGATOR_EVENT.getValue(), getPublicationID(), getBusServer()));

    	System.out.println("Aggregator initialized");
	}
}