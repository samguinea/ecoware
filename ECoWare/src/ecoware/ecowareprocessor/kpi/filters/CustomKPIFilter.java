package ecoware.ecowareprocessor.kpi.filters;

import ecoware.ecowareprocessor.eventlisteners.KPIEventListener;
import ecoware.ecowareprocessor.kpi.KPIManager;
import ecoware.util.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Set;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

/**
 * 
 * @author Armando Varriale
 * <br/><br/>
 * This class is an implementation of a "Custom KPI Filter". It extends the <a href="KPIManager.html">KPIManager</a> 
 * abstract class by adding some specific aspects and provide an adequate implementation of the "launch()" method.
 * <br/><br/>
 * This class let you to create a custom KPI filter, using a XML file for filter specification.<br/><br/>
 * In particular you must provide a set of parameters like the custom filter name, the event name to which the filter 
 * must be applied, the publication ID and the subscription IDs, and finally the EPL queries (at least one) that 
 * realize the filtration task.<br/>
 * 
 * For a more detailed presentation of these concepts, the creation and the relative usage of a custom filter, 
 * see the provided <a href="">tutorials </a>section of the ECoWare documentation.
 *
 */
public class CustomKPIFilter extends KPIManager {

	private String filterName;
	private String eventName;
	private ArrayList<String> esperStatements;
	
	/**
	 * Constructs a new CustomKPIFilter using the specified XML element. The bus server name (that is 
	 * the hostname on which the bus server is running) and Esper configuration are also required.
	 * @param xmlElement the XML element (node) of the configuration file from which retrieve the information to build the KPI object
	 * @param busServer the hostname on which the bus server is running
	 * @param esperConfiguration the Esper current configuration (that is an Configuration object. For further detail see the <a href="http://esper.codehaus.org/" target="_blank">Esper</a> documentation).
	 */
	public CustomKPIFilter(Element xmlElement, String busServer, Configuration esperConfiguration) throws Exception {
		super(xmlElement, busServer, esperConfiguration);

		filterName = xmlElement.getElementsByTagName("filter_name").item(0).getTextContent();
		eventName = xmlElement.getElementsByTagName("event_name").item(0).getTextContent();
		
		esperStatements = new ArrayList<String>();
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
	}

	@Override
	/**
	 * This method actually starts the "Custom" filter.<br/>
	 */
	public void launch() {		
		Logger.logInfo("------------------");
		Logger.logInfo("Initializing " + filterName + " filter...");
		
		//ESPER configuration
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(getEsperConfiguration());		
		EPStatement tempStatement = null;

		for(int i=0; i < esperStatements.size(); i++)
			tempStatement = epService.getEPAdministrator().createEPL(esperStatements.get(i));
		
		tempStatement.addListener(new KPIEventListener(eventName, getPublicationID(), getBusServer()));		
		Logger.logInfo("Filter initialized");
	}
}