package ecoware.ecowareprocessor.kpi;

import java.util.ArrayList;
import org.w3c.dom.Element;
import com.espertech.esper.client.Configuration;

/**
 * 
 * @author Armando Varriale
 * <br/><br/>
 * This is an abstract class that represent a generic KPI object. More precisely, it defines 
 * methods and properties that characterize all kind of KPI.<br/><br/>
 * The KPI construction is based on the configuration file that is submitted to 
 * the <a href="ECoWareProcessor.html">ECoWareProcessor</a> object.
 */
public abstract class KPIManager {
	private ArrayList<String> subscriptionIDs;
	private String publicationID;
	private String busServer;
	private Configuration esperConfiguration;
	
	/**
	 * Constructs a new KPIManager using the specified XML element. The bus server name (that is 
	 * the hostname on which the bus server is running) and Esper configuration are also required.
	 * @param xmlElement the XML element (node) of the configuration file from which retrieve the information to build the KPI object
	 * @param busServer the hostname on which the bus server is running
	 * @param esperConfiguration the Esper current configuration (that is an Configuration object. For further detail see the <a href="http://esper.codehaus.org/" target="_blank">Esper</a> documentation).
	 */
	public KPIManager(Element xmlElement, String busServer, Configuration esperConfiguration) {		
		subscriptionIDs = new ArrayList<String>();
		
		for(int i = 0; i < xmlElement.getElementsByTagName("subscriptID").getLength(); i++)
			subscriptionIDs.add(xmlElement.getElementsByTagName("subscriptID").item(i).getTextContent());

		publicationID = xmlElement.getElementsByTagName("publicationID").item(0).getTextContent();		
		this.busServer = busServer;
		this.esperConfiguration = esperConfiguration;
	}
	
	/**
	 * Returns the list of subscribe keys. The list is an ArrayList of strings. 
	 * @return the subscribe keys list
	 */
	public ArrayList<String> getSubscriptionIDs() {
		return subscriptionIDs;
	}
	
	/**
	 * Returns the publication ID.
	 * @return the publication ID.
	 */
	public String getPublicationID() {		
		return publicationID;
	}
	
	/**
	 * Return the hostname on which the bus server is running.
	 * @return the ECoWare bus server
	 */
	public String getBusServer() {
		return busServer;
	}
	
	/**
	 * Return the Esper configuration currently in use..
	 * @return the Esper configuration
	 */
	public Configuration getEsperConfiguration() {
		return esperConfiguration;
	}
	
	/**
	 * This method actually starts the KPI processing.<br/>
	 * This is an abstract method that all classes that extend KPIManager must implement.
	 * @throws Exception
	 */
	public abstract void launch() throws Exception;
}