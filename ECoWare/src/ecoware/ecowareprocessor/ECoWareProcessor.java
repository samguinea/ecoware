package ecoware.ecowareprocessor;

import java.io.InputStream;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;

import ecoware.ecowareprocessor.kpi.*;
import ecoware.ecowareprocessor.kpi.aggregators.*;
import ecoware.ecowareprocessor.kpi.calculators.*;
import ecoware.ecowareprocessor.kpi.filters.*;
import ecoware.ecowareaccessmanager.*;

/**
 * @author Armando Varriale
 * <br/><br/>
 * This class represent the ECoWare processor that is the entity by which you can create and generate processing 
 * units to perform analysis you need (eg. KPI processors).<br/>
 * So all you have to do is create a new ECoWareProcess object, passing to a certain configuration file 
 * (related to your KPIs configuration or the KPIs set you need for your system analysis), and then start 
 * the created object to begin system probing.
 */
public class ECoWareProcessor {
	
	private InputStream configurationFile;
	private String busServer;
	private ECoWareMessageReceiver receiver;
	private ArrayList<String> subscriptions;
	private DocumentBuilderFactory docFactory;
	private DocumentBuilder docBuilder;
	private Document configurationDocument;
	private Configuration esperConfiguration;
	private EPServiceProvider esperServiceProvider;
	private Element kpiElement;
	private String elementName;
	private KPIManager kpi;
	
	
//	public ECoWareProcess() throws Exception{
//		this(null);
//	}

	/**
	 * Constructs a new ECoWareProcess with the supplied configuration file. The configuration file is an XML file 
	 * which has all the information regarding to the processor to build as explained in the ECoWare project Wiki. 
	 * @param configurationFile the configuration file that specify the needed KPIs for the analysis 
	 * @throws Exception 
	 */
	public ECoWareProcessor(InputStream configurationFile) throws Exception {
		this.configurationFile = configurationFile;
		
		if(getConfigurationFile()==null) throw new Exception("Error: configuration file not specified. A configuration file is required to create an ECoWare process.");
		
		docFactory = DocumentBuilderFactory.newInstance();
		docBuilder = docFactory.newDocumentBuilder();
		configurationDocument = docBuilder.parse(this.configurationFile);
		subscriptions = new ArrayList<String>(0);
		
		Element confRoot = configurationDocument.getDocumentElement();
		busServer = confRoot.getElementsByTagName("ecowareAccessManagerUrl").item(0).getTextContent();
		esperConfiguration = new Configuration();
		esperServiceProvider = EPServiceProviderManager.getDefaultProvider(esperConfiguration);
		
		// calculators creations
		NodeList kpiList = confRoot.getElementsByTagName("Calculator");
		for(int i=0; i<kpiList.getLength(); i++){
			kpiElement = (Element) kpiList.item(i);
			elementName = kpiElement.getElementsByTagName("name").item(0).getTextContent();
			//System.out.print("Element name: " + elementName);
			kpi = createCalculator(elementName, kpiElement, busServer, esperConfiguration);
			subscriptions.addAll(kpi.getSubscriptionIDs());
			kpi.launch();
		}
		
		// custom calculators creations
		NodeList customList = confRoot.getElementsByTagName("Custom_Calculator");
		for(int i=0; i<customList.getLength(); i++){
			kpiElement = (Element) customList.item(i);
			//System.out.print("Element name: " + elementName);
			kpi = new CustomKPICalculator(kpiElement, busServer, esperConfiguration);
			subscriptions.addAll(kpi.getSubscriptionIDs());
			kpi.launch();
		}
		
		// filters creations
		NodeList filterList = confRoot.getElementsByTagName("Filter");
		for(int i=0; i<filterList.getLength(); i++){
			kpiElement = (Element) filterList.item(i);
			elementName = kpiElement.getElementsByTagName("name").item(0).getTextContent();
			//System.out.print("Element name: " + elementName);
			kpi = createFilter(elementName, kpiElement, busServer, esperConfiguration);
			subscriptions.addAll(kpi.getSubscriptionIDs());
			kpi.launch();
		}
		
		// custom filters creations
		NodeList customFilterList = confRoot.getElementsByTagName("Custom_Filter");
		for(int i=0; i<customFilterList.getLength(); i++){
			kpiElement = (Element) customFilterList.item(i);
			//System.out.print("Element name: " + elementName);
			kpi = new CustomKPIFilter(kpiElement, busServer, esperConfiguration);
			subscriptions.addAll(kpi.getSubscriptionIDs());
			kpi.launch();
		}
		
		// aggregator creation
		NodeList aggregatorList = confRoot.getElementsByTagName("Aggregator");
		for(int i=0; i<aggregatorList.getLength(); i++){
			kpiElement = (Element) aggregatorList.item(i);
			//System.out.println("Element name: " + elementName);
			kpi = new Aggregator_Draft(kpiElement, busServer, esperConfiguration);
			subscriptions.addAll(kpi.getSubscriptionIDs());
			kpi.launch();
		}
		
		receiver = new ECoWareMessageReceiver(busServer, subscriptions);
		receiver.addECoWareMessageListener(new ProcessListener(esperServiceProvider));
	}
	
	/**
	 * Starts the processor.
	 * @throws Exception
	 */
	public void start() throws Exception{
		this.receiver.startConnection();
	}
	
	// Create calculators
	private KPIManager createCalculator(String elementName, Element kpiElement, String busServer, Configuration esperConfiguration) {
		KPIManager tmpKPI = null;
		switch (elementName) {
			case "AvgRT": // Response Time Calculator
				tmpKPI = new AvgRTCalculator(kpiElement, busServer, esperConfiguration);
				break;
	
			case "ArrivalRate": // Arrival Rate Calculator
				tmpKPI = new ArrivalRateCalculator(kpiElement, busServer, esperConfiguration);
				break;
			
			case "Reliability": // Reliability Calculator
				tmpKPI = new ReliabilityCalculator(kpiElement, busServer, esperConfiguration);
				break;
				
			default:
				break;
		}
		return tmpKPI;
	}
	
	// Create filters
	private KPIManager createFilter(String elementName, Element kpiElement, String busServer, Configuration esperConfiguration) {
		KPIManager tmpKPI = null;
		switch (elementName) {
		case "HPFilter": // High Pass Filter
			tmpKPI = new HPFilter(kpiElement, busServer, esperConfiguration);
			break;

		case "LPFilter": // Low Pass Filter
			tmpKPI = new LPFilter(kpiElement, busServer, esperConfiguration);
			break;

		default: 
			break;
		}
		return tmpKPI;
	}
	

	/**
	 * Return the configuration file used to generate the processor.
	 * @return the url of the configuration file
	 */
	public InputStream getConfigurationFile() {
		return configurationFile;
	}

//	/**
//	 * Sets the configuration file used to generate the processor.
//	 * @param configurationFile the configuration file to set
//	 */
//	public void setConfigurationFile(InputStream configurationFile) {
//		this.configurationFile = configurationFile;
//	}

	/**
	 * Return the hostname on which the bus server is running.
	 * @return the ECoWare bus server
	 */
	public String getBusServer() {
		return busServer;
	}

//	/**
//	 * @param busServer the url to set for the ECoWare bus server
//	 */
//	public void setBusServer(String busServer) {
//		this.busServer = busServer;
//	}
	
}
