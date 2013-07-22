package ecoware.ecowareprocessor.kpi.calculators;

import ecoware.ecowareprocessor.eventlisteners.KPIEventListener;
import ecoware.ecowareaccessmanager.ECoWareEventType;
import ecoware.util.*;
import java.util.HashMap;
import java.util.Map;
import org.w3c.dom.Element;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

/**
 * 
 * @author Armando Varriale
 * <br/><br/>
 * This class is an implementation of an <b>"Arrival Rate"</b> calculator (KPI).
 * It extends the <a href="StandardKPICalculator.html">StandardKPICalculator</a> abstract class by 
 * providing an adequate implementation of the "launch()" method.<br/><br/>
 * 
 * An <b>"Arrival Rate"</b> calculator is a processor that produce an estimation of the arrival rate related to one or 
 * more specific services, that are those present in the subscribe keys list. More precisely, it collects the 
 * "START_TIME" events (that are sent by subscribed services/tasks) and counts how many events fall off inside the inspection 
 * window (see <a href="StandardKPICalculator.html">StandardKPICalculator</a> for more details on the inspection 
 * window). Finally, according to the specified output frequency (that is the "output production" parameter, 
 * see <a href="StandardKPICalculator.html">StandardKPICalculator</a> for more details on it), it sends on the bus 
 * an "ARRIVALRATE_EVENT" event that contains data relative to the calculated arrival rate (that is, the count).
 * 
 * In ECoWare (and Esper too) an event is modeled as a "HashMap&lt;String, Object&gt;", so its content is a set 
 * of "&lt;key, value&gt;" pairs. Each event has its specific map that is required to make possible their correct usage 
 * during analysis processes. <br/><br/>
 * 
 * As said, the <i>"Arrival Rate"</i> calculator requires in input (a set of) </i>"START_TIME"</i> events (the start 
 * time related to a certain operation/service that you want monitor/analyze) while produces in output an 
 * </i>"ARRIVALRATE_EVENT"</i> event with the calculated arrival rate.<br/><br/>
 * 
 ** For a <i>"Arrival Rate"</i> calculator, a <b>"START_TIME"</b> event is defined by this map:
 * <UL>
 *  <LI>&lt;"key", String.class&gt;
 *  <LI>&lt;"value", long.class&gt;
 * </UL>
 * <br/>
 * that is, the name of the first element of the map is "key" and its type is "String", while the name
 * of the second element of the map is "value" and its type is "long".
 * 
 * An <b>"ARRIVALRATE_EVENT"</b> event is defined by this map:
 * <UL>
 *  <LI>&lt;"timestamp", long.class&gt;
 *  <LI>&lt;"value", double.class&gt;
 *  <LI>&lt;"source", String.class&gt;
 * </UL>
 * <br/>
 * that is, the name of the first element of the map is "timestamp" and its type is "long", while the name
 * of the second element of the map is "value" and its type is "double". The last element of the map is "source" and its type is "String".<br/><br/>
 * 
 * For a more detailed presentation of these concepts, see the provided <a href="">tutorials </a>section of the ECoWare documentation.
 * 
 *
 */
public class ArrivalRateCalculator extends StandardKPICalculator {

	/**
	 * Constructs a new ArrivalRateCalculator using the specified XML element. The bus server name (that is 
	 * the hostname on which the bus server is running) and Esper configuration are also required.
	 * @param xmlElement the XML element (node) of the configuration file from which retrieve the information to build the KPI object
	 * @param busServer the hostname on which the bus server is running
	 * @param esperConfiguration the Esper current configuration (that is an Configuration object. For further detail see the <a href="http://esper.codehaus.org/" target="_blank">Esper</a> documentation).
	 */
	public ArrivalRateCalculator(Element xmlElement, String busServer, Configuration esperConfiguration) {
		super(xmlElement, busServer, esperConfiguration);
	}

	@Override
	/**
	 * This method actually starts the "ArrivalRateCalculator" KPI processing.<br/>
	 */
	public void launch() {		
		Logger.logInfo("---");
		Logger.logInfo("Arrival Rate");
		Logger.logInfo("Initializing calculator...");
		
        //ESPER configuration
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(getEsperConfiguration());

        //map for StartTime and EndTime events
		Map<String, Object> timeEventMap = new HashMap<String, Object>();		
		timeEventMap.put("key", String.class);
		timeEventMap.put("value", long.class);
		
		epService.getEPAdministrator().getConfiguration().addEventType(ECoWareEventType.START_TIME.getValue(), timeEventMap);
		
		//KPI map for filters
	    Map<String, Object> filterMap = new HashMap<String, Object>();
	    filterMap.put("timestamp", long.class);
	    filterMap.put("value", double.class);
	    filterMap.put("source", String.class);

		epService.getEPAdministrator().getConfiguration().addEventType(ECoWareEventType.ARRIVALRATE_EVENT.getValue(), filterMap);

		//EPL creation
		//ESPER statement generation
		String esperStatement = "SELECT COUNT(*) AS value, current_timestamp() AS timestamp, '"+getPublicationID()+"' as source " +
				"FROM StartTime.win:time(" + getIntervalValue() + " " + getIntervalUnit() + ") " +
				"OUTPUT SNAPSHOT EVERY " + getOutputValue() + " " + getOutputUnit();

		EPStatement eplStatement = epService.getEPAdministrator().createEPL(esperStatement);		
		eplStatement.addListener(new KPIEventListener(ECoWareEventType.ARRIVALRATE_EVENT.getValue(), getPublicationID(), getBusServer()));
		Logger.logInfo("Calculator initialized");
	}
}