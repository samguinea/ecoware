package ecoware.ecowareprocessor.kpi.filters;

import ecoware.ecowareprocessor.eventlisteners.KPIEventListener;
import ecoware.ecowareaccessmanager.ECoWareEventType;

import org.w3c.dom.Element;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

/**
 * 
 * @author Armando Varriale
 * <br/><br/>
 * This class is an implementation of a <b>"Low Pass Filter"</b>.
 * It extends the <a href="StandardKPIFilter.html">StandardKPIFilter</a> abstract class by 
 * adding some specific properties that characterize a <i>"Low Pass Filter"</i> and providing an 
 * adequate implementation of the "launch()" method.<br/><br/>
 * 
 * The properties that have been introduced refer to the following concept:
 * <ul>
 *  <li> "event name" (is a string value), that is the name of the event to which the filter must be applied
 *  <li> "attribute name" (is a string value), that is the event attribute to filter
 *  <li> "cutoff" (is a double value), that is the upper limit value used to determine the accepted values, 
 *  in fact the values that exceed this limit will be discarded. 
 * </ul
 * 
 * Obviously, these parameters must be provided (to the ECoWareProcessor) by the XML configuration file.
 * 
 * As can be easily understood, a low pass filter is a filter that accepts all values that are underneath the 
 * upper limit and rejects all other values. So the "cutoff" limit is used to determine the accepted 
 * values (and, by the way, the rejected values).
 * 
 * For a more detailed presentation of filters usage, see the provided <a href="">tutorials </a>section of the ECoWare documentation.
 *
 */
public class LPFilter extends StandardKPIFilter {

	private String eventName;
	private String attributeName;
	private double cutoff;
	
	/**
	 * Constructs a new LPFilter using the specified XML element. The bus server name (that is 
	 * the hostname on which the bus server is running) and Esper configuration are also required.
	 * @param xmlElement the XML element (node) of the configuration file from which retrieve the information to build the KPI object
	 * @param busServer the hostname on which the bus server is running
	 * @param esperConfiguration the Esper current configuration (that is an Configuration object. For further detail see the <a href="http://esper.codehaus.org/" target="_blank">Esper</a> documentation).
	 */
	public LPFilter(Element xmlElement, String busServer, Configuration esperConfiguration) {

		super(xmlElement, busServer, esperConfiguration);
		
		eventName = xmlElement.getElementsByTagName("eventName").item(0).getTextContent();
		attributeName = xmlElement.getElementsByTagName("attributeName").item(0).getTextContent();
		cutoff = Double.parseDouble(xmlElement.getElementsByTagName("cutoff").item(0).getTextContent());
	}

	@Override
	/**
	 * This method actually starts the "Low Pass" filter.<br/>
	 */
	public void launch() {
		
		System.out.println("---");
		System.out.println("Initializing low-pass filter...");
		
        //ESPER configuration
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(getEsperConfiguration());
		
		String expression = "SELECT * " +
			"FROM " + eventName + ".win:length(1)" +
			"WHERE " + attributeName + " < " + cutoff;

		EPStatement statement = epService.getEPAdministrator().createEPL(expression);

		statement.addListener(new KPIEventListener(ECoWareEventType.getEventType(eventName).getValue(), getPublicationID(), getBusServer()));
		
		System.out.println("Filter initialized");
	}
}