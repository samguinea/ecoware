package ecoware.ecowareprocessor.kpi.aggregators;

import ecoware.ecowareaccessmanager.ECoWareEventType;
import ecoware.ecowareprocessor.kpi.KPIManager;
import ecoware.ecowareprocessor.eventlisteners.AggregatorEventListener;
import ecoware.util.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Set;
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
 * This class is an implementation of an "Aggregator KPI". It extends the <a href="KPIManager.html">KPIManager</a> 
 * abstract class by adding some specific aspects and provide an adequate implementation of the "launch()" method.
 * 
 * An "Aggregator" calculator is a processor that aggregates one or more events among them, correlating a set 
 * of secondary events to a specific primary event.
 * 
 * For a more detailed presentation of these concepts, see the provided <a href="">Getting Started</a> section of the ECoWare documentation.
 *
 */
public class Aggregator extends KPIManager {
	
	private String aggregatorName;
	private String primaryEventName;
	private String primaryEventKey;
	private String secondaryEventName;
	private NodeList secondaryEvents;
	private Map<String, Object> secondaryEventsList;
	private ArrayList<String> eventSpec;
	private Element eventNode;
	
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
		primaryEventKey = xmlElement.getElementsByTagName("primaryEventKey").item(0).getTextContent();
		secondaryEvents = xmlElement.getElementsByTagName("secondaryEvent");
		if(secondaryEvents.getLength()<1) throw new Exception("For an aggregator event at least one secondary event must be declared!");
		
		secondaryEventsList = new LinkedHashMap<String, Object>(0);
		for(int i=0; i<secondaryEvents.getLength(); i++){
			eventSpec =  new ArrayList<String>(0);
			eventNode = (Element) secondaryEvents.item(i);
			secondaryEventName = eventNode.getElementsByTagName("secondaryEventName").item(0).getTextContent();
			eventSpec.add(secondaryEventName);
			eventSpec.add(eventNode.getElementsByTagName("secondaryEventKey").item(0).getTextContent());
			eventSpec.add(eventNode.getElementsByTagName("subscriptID").item(0).getTextContent());
			eventSpec.add(eventNode.getElementsByTagName("intervalUnit").item(0).getTextContent());
			eventSpec.add(eventNode.getElementsByTagName("intervalValue").item(0).getTextContent());
			secondaryEventsList.put(secondaryEventName+"_"+eventSpec.get(2), eventSpec);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	/**
	 * This method actually starts the "Aggregator" KPI processing.<br/>
	 */
	public void launch() {
		Logger.logInfo("---");
		Logger.logInfo("Initializing aggregator " + aggregatorName + "...");
		
        //ESPER configuration
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(getEsperConfiguration());

//        //map for infrastructure events
		
//		for(String secondEvent: secondEvents){
//			if(!epService.getEPAdministrator().getConfiguration().isEventTypeExists(secondEvent)) {
//				// secondary event map
//				Map<String, Object> secondaryEventMap = new HashMap<String, Object>();
//				secondaryEventMap.put("originID", String.class);
//				epService.getEPAdministrator().getConfiguration().addEventType(secondEvent, secondaryEventMap);
//			}
//		}
		
		// ESPER statement generation
		String sStatement = "SELECT PrimaryEvent_" + primaryEventName + "_" + getSubscriptionIDs().get(0) + ".timestamp, PrimaryEvent_" + primaryEventName + "_" + getSubscriptionIDs().get(0) + "." + primaryEventKey;
		String fStatement = " FROM pattern [every PrimaryEvent_" + primaryEventName + "_" + getSubscriptionIDs().get(0) + "="+ primaryEventName +"] as PrimaryEvent_" + primaryEventName + "_" + getSubscriptionIDs().get(0) + " unidirectional";
		Set<String> secondEvents = secondaryEventsList.keySet();
		ArrayList<String> secondEventSpecs;
		
		for(String secondEvent: secondEvents){
			secondEventSpecs = (ArrayList<String>)secondaryEventsList.get(secondEvent);
			//statement creation
			String cwStatement = String.format("create window %s.win:time(%s %s) as select %s from %s", 
					secondEvent, secondEventSpecs.get(4), secondEventSpecs.get(3), secondEventSpecs.get(1), secondEventSpecs.get(0));
			epService.getEPAdministrator().createEPL(cwStatement);
			
			String oiwStatement = String.format("on %s(source='%s') insert into %s select %s", 
					secondEventSpecs.get(0), secondEventSpecs.get(2), secondEvent, secondEventSpecs.get(1));
			epService.getEPAdministrator().createEPL(oiwStatement);
			
			sStatement += ", " + secondEvent+ "." + secondEventSpecs.get(1);
			fStatement += ", " + secondEvent;
		}
		
		String esperStatement = sStatement + fStatement;
		EPStatement eplStatement = epService.getEPAdministrator().createEPL(esperStatement);
		eplStatement.addListener(new AggregatorEventListener(ECoWareEventType.AGGREGATOR_EVENT.getValue(), getPublicationID(), getBusServer()));
		Logger.logInfo("Aggregator initialized");
	}
}