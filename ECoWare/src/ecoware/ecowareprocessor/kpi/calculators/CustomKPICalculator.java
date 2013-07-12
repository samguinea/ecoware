package ecoware.ecowareprocessor.kpi.calculators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import ecoware.ecowareprocessor.eventlisteners.KPIEventListener;
import ecoware.ecowareprocessor.kpi.KPIManager;
import ecoware.util.*;

/**
 * 
 * @author Armando Varriale
 * <br/><br/>
 * This class is an implementation of a "Custom KPI Calculator". It extends the <a href="KPIManager.html">KPIManager</a> 
 * abstract class by adding some specific aspects and provide an adequate implementation of the "launch()" method.
 * <br/><br/>
 * This class let you to create a custom KPI calculator, using a XML file for calculator specification.<br/><br/>
 * In particular you must provide a set of parameters like the custom calculator name, the publication ID and the 
 * subscription IDs, the events list that are involved into the calculation and finally the EPL queries (at least one) 
 * that realize the desired computation.<br/>
 * 
 * For a more detailed presentation of these concepts, the creation and the relative usage of a custom calculato (KPI), 
 * see the provided <a href="">tutorials </a>section of the ECoWare documentation.
 * 
 *
 */

public class CustomKPICalculator extends KPIManager {
	
	private String kpiName;
	private int kpiId;
	private ArrayList<String> esperStatements;
	private ArrayList<String> eventNames;
	private ArrayList<Map <String, Object>> eventMaps;

	/**
	 * Constructs a new CustomKPICalculator using the specified XML element. The bus server name (that is 
	 * the hostname on which the bus server is running) and Esper configuration are also required.
	 * @param xmlElement the XML element (node) of the configuration file from which retrieve the information to build the KPI object
	 * @param busServer the hostname on which the bus server is running
	 * @param esperConfiguration the Esper current configuration (that is an Configuration object. For further detail see the <a href="http://esper.codehaus.org/" target="_blank">Esper</a> documentation).
	 */
	public CustomKPICalculator(Element xmlElement, String busServer, Configuration esperConfiguration) throws Exception {
		super(xmlElement, busServer, esperConfiguration);
		
		kpiName = xmlElement.getElementsByTagName("calculator_name").item(0).getTextContent();
		kpiId = Integer.parseInt(xmlElement.getElementsByTagName("id").item(0).getTextContent());
		
		esperStatements = new ArrayList<String>(0);
		NodeList epls = xmlElement.getElementsByTagName("EPL");
		Node epl_statement;
		Node statement;
		if(epls.getLength() == 0) throw new Exception("No query found in xml file!");
		for(int j = 0; j < epls.getLength(); j++){
			epl_statement = epls.item(j);		
			for(int k=0; k < epl_statement.getChildNodes().getLength(); k++){
				statement = epl_statement.getChildNodes().item(k);
				if(statement.getNodeType() == Node.ELEMENT_NODE){
					Logger.logInfo("------------------");
					Logger.logInfo("Element name: " + statement.getNodeName());
					if(statement.getNodeName().toLowerCase().equals("query_statement")){
						esperStatements.add(statement.getTextContent());
						Logger.logDebug("Value: " + statement.getTextContent());
					}
					else{
						if(statement.getNodeName().toLowerCase().equals("composite_statement")){
							LinkedHashMap<String, Object> query = new LinkedHashMap<String, Object>(0);
							for(int i=0; i<statement.getChildNodes().getLength(); i++){
								if(statement.getChildNodes().item(i).getNodeType() == Node.ELEMENT_NODE){
									Logger.logDebug(statement.getChildNodes().item(i).getNodeName() + " " + statement.getChildNodes().item(i).getTextContent());
									query.put(statement.getChildNodes().item(i).getNodeName(), statement.getChildNodes().item(i).getTextContent()); // aggiungo pezzo
								}
							}
							if(query.isEmpty()) throw new Exception("Bad query description in xml file!");
							else {
								Set<String> queryKeyword = query.keySet();
								String compositeQuery = "";
								for(String keyword : queryKeyword){
									switch(keyword){
										case "insert_into": compositeQuery += "INSERT INTO " + query.get(keyword) + " ";
															break;
										case "select": 	compositeQuery += "SELECT " + query.get(keyword) + " ";
													   	break;
										case "from": 	compositeQuery += "FROM " + query.get(keyword) + " ";
										   				break;
										case "where": 	compositeQuery += "WHERE " + query.get(keyword) + " ";
														break;
										case "group_by": compositeQuery += "GROUP BY " + query.get(keyword) + " ";
														 break;
										case "having": compositeQuery += "HAVING " + query.get(keyword) + " ";
													   break;
										case "output_every": compositeQuery += "OUTPUT SNAPSHOT EVERY " + query.get(keyword) + " ";
															 break;
										default: throw new Exception("Not valid query statement: " + keyword);
									}
								}
								Logger.logDebug("Value: " + compositeQuery);
								esperStatements.add(compositeQuery);
							}
						}
					}
				}
			}
		}
		
		eventNames = new ArrayList<String>();
		eventMaps = new ArrayList<Map<String, Object>>();

		NodeList eventList = xmlElement.getElementsByTagName("event");
		Element tempEvent;
		if(eventList.getLength() == 0) throw new Exception("No events found in xml file!");	
		for(int j = 0; j < eventList.getLength(); j++) {			
			tempEvent = (Element)eventList.item(j);
			eventNames.add(tempEvent.getElementsByTagName("name").item(0).getTextContent());
			eventMaps.add(getEventMap((Element)tempEvent.getElementsByTagName("map").item(0)));
		}
	}

	private Map<String, Object> getEventMap(Element mapElement) {
		Map<String, Object> eventMap = new HashMap<String, Object>();		
		NodeList attributeList = mapElement.getElementsByTagName("attribute");		
		String tempAttributeName, tempAttributeType;
		
		for(int i=0; i < attributeList.getLength(); i++) {			
			tempAttributeName = ((Element)attributeList.item(i)).getElementsByTagName("name").item(0).getTextContent();
			tempAttributeType = ((Element)attributeList.item(i)).getElementsByTagName("type").item(0).getTextContent();
			
			if(tempAttributeType.equals("int"))
				eventMap.put(tempAttributeName, int.class);
			
			if(tempAttributeType.equals("long"))
				eventMap.put(tempAttributeName, long.class);
			
			if(tempAttributeType.equals("double"))
				eventMap.put(tempAttributeName, double.class);
			
			if(tempAttributeType.equals("String"))
				eventMap.put(tempAttributeName, String.class);
		}		
		return eventMap;
	}

	@SuppressWarnings({ })
	@Override
	/**
	 * This method actually starts the "CustomKPICalculator" processing.<br/>
	 * @throws Exception
	 */
	public void launch() throws Exception {
		Logger.logInfo("------------------");
		Logger.logInfo(kpiName + " calculator");
		Logger.logInfo("Initializing calculator...");
        
        //ESPER configuration
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(getEsperConfiguration());

		//event adding
		for(int k=0; k < eventNames.size(); k++)
			epService.getEPAdministrator().getConfiguration().addEventType(eventNames.get(k), eventMaps.get(k));

		EPStatement tempStatement = null;
		
		for(int k=0; k < esperStatements.size(); k++)
			tempStatement = epService.getEPAdministrator().createEPL(esperStatements.get(k));
		
		tempStatement.addListener(new KPIEventListener(kpiName, getPublicationID(), getBusServer(), kpiId));
		Logger.logInfo("Calculator initialized");
	}
}