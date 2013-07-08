package ecoware.ecowareprocessor.kpi.filters;

import ecoware.ecowareprocessor.kpi.KPIManager;

import org.w3c.dom.Element;

import com.espertech.esper.client.Configuration;

/**
 * 
 * @author Armando Varriale
 * <br/><br/>
 * This is an abstract class that provide an implementation of a standard KPI filter. It extends the <a href="KPIManager.html">KPIManager</a> 
 * abstract class by adding some specific aspects (methods and properties) that characterize all KPI filters, but doesn't 
 * implement the "launch()" method (that will be implemented by specific subclasses).<br/>
 *
 */
public abstract class StandardKPIFilter extends KPIManager {

	/**
	 * Constructs a new StandardKPIFilter using the specified XML element. The bus server name (that is 
	 * the hostname on which the bus server is running) and Esper configuration are also required.
	 * @param xmlElement the XML element (node) of the configuration file from which retrieve the information to build the KPI object
	 * @param busServer the hostname on which the bus server is running
	 * @param esperConfiguration the Esper current configuration (that is an Configuration object. For further detail see the <a href="http://esper.codehaus.org/" target="_blank">Esper</a> documentation).
	 */
	public StandardKPIFilter(Element xmlElement, String busServer, Configuration esperConfiguration) {
		
		super(xmlElement, busServer, esperConfiguration);
	}
}