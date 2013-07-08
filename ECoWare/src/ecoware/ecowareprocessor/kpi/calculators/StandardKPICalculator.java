package ecoware.ecowareprocessor.kpi.calculators;

import ecoware.ecowareprocessor.kpi.KPIManager;

import org.w3c.dom.Element;

import com.espertech.esper.client.Configuration;

/**
 * 
 * @author Armando Varriale
 * <br/><br/>
 * This is an abstract class that provide an implementation of a standard calculator KPI. It extends the <a href="KPIManager.html">KPIManager</a> 
 * abstract class by adding some specific aspects (methods and properties) that characterize all calculator KPIs, but doesn't 
 * implement the "launch()" method (that will be implemented by specific subclasses).<br/><br/>
 * 
 * The aspects that have been introduced refer to the concept of  "inspection interval" and "output production". The first 
 * is the window size used to collect data to process with Esper, while the second is the frequency by which the processed 
 * data are pulled out and sent to the application.<br/>
 * The "inspection interval" is characterized by "interval unit" (eg. unit of time) and "interval value" (eg. a positive 
 * number); so if "interval unit" is "seconds" and "interval value" is "30" it means that the valuation window size is 30 seconds.<br/><br/>
 * The "output production" is characterized by "output unit" (eg. unit of time) and "output value" (eg. a positive 
 * number); so if "output unit" is "seconds" and "output value" is "5" it means that data are pulled out every 5 seconds.
 *
 */
public abstract class StandardKPICalculator extends KPIManager {

	private String intervalUnit;
	private int intervalValue;
	private String outputUnit;
	private int outputValue;
	
	/**
	 * Constructs a new StandardKPICalculator using the specified XML element. The bus server name (that is 
	 * the hostname on which the bus server is running) and Esper configuration are also required.
	 * @param xmlElement the XML element (node) of the configuration file from which retrieve the information to build the KPI object
	 * @param busServer the hostname on which the bus server is running
	 * @param esperConfiguration the Esper current configuration (that is an Configuration object. For further detail see the <a href="http://esper.codehaus.org/" target="_blank">Esper</a> documentation).
	 */
	public StandardKPICalculator(Element xmlElement, String busServer, Configuration esperConfiguration) {
		
		super(xmlElement, busServer, esperConfiguration);
		
		Element computation = (Element)xmlElement.getElementsByTagName("computation").item(0);
		intervalUnit = computation.getElementsByTagName("intervalUnit").item(0).getTextContent();
		intervalValue = Integer.parseInt(computation.getElementsByTagName("intervalValue").item(0).getTextContent());
		outputUnit = computation.getElementsByTagName("outputUnit").item(0).getTextContent();
		outputValue = Integer.parseInt(computation.getElementsByTagName("outputValue").item(0).getTextContent());
	}

	/**
	 * Returns the interval unit used in the Esper query.
	 * @return the interval unit
	 */
	public String getIntervalUnit() {
		return intervalUnit;
	}

	/**
	 * Returns the interval value used in the Esper query.
	 * @return the interval value
	 */
	public int getIntervalValue() {		
		return intervalValue;
	}

	/**
	 * Returns the output unit used in the Esper query (eg. how often data are retrieved).
	 * @return the output unit
	 */
	public String getOutputUnit() {		
		return outputUnit;
	}

	/**
	 * Returns the output value used in the Esper query (eg. how often data are retrieved).
	 * @return the output value
	 */
	public int getOutputValue() {		
		return outputValue;
	}
}