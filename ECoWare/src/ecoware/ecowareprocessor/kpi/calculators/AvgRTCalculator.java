package ecoware.ecowareprocessor.kpi.calculators;

import ecoware.ecowareprocessor.eventlisteners.KPIEventListener;
import ecoware.ecowareaccessmanager.ECoWareEventType;

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
 * This class is an implementation of an <b>"Average Response Time"</b> calculator (KPI).
 * It extends the <a href="StandardKPICalculator.html">StandardKPICalculator</a> abstract class by 
 * providing an adequate implementation of the "launch()" method.<br/><br/>
 * 
 * An <b>"Average Response Time"</b> calculator is a processor that compute the average response time related to one or 
 * more specific services, that are those present in the subscribe keys list. More precisely, it collects two events, 
 * a "START_TIME" event and a "END_TIME" event (that are sent by subscribed services/tasks) and computes the difference between 
 * these values; then evaluates the average of the differences inside the inspection window (see 
 * <a href="StandardKPICalculator.html">StandardKPICalculator</a> for more details on the inspection window). Finally, 
 * according to the specified output frequency (that is the "output production" parameter, see 
 * <a href="StandardKPICalculator.html">StandardKPICalculator</a> for more details on it), it sends on the bus an 
 * "AVGRT_EVENT" event that contains data relative to the calculated average response time.<br/><br/>
 * 
 * In ECoWare (and Esper too) an event is modeled as a “HashMap<String, Object>”, so its content is a set 
 * of “<key, value>” pairs. Each event has its specific map that is required to make possible their correct usage 
 * during analysis processes. <br/><br/>
 * 
 * As said, the <i>"Average Response Time"</i> calculator requires in input </i>"START_TIME"</i> and </i>"END_TIME"</i> 
 * events (the start and the end time related to a certain operation/service that you want monitor/analyze) while 
 * produces in output an </i>"AVGRT_EVENT"</i> event with the calculated average response time.<br/><br/>
 * 
 * For a <i>"Average Response Time"</i> calculator, a <b>"START_TIME"</b> event is defined by this map:
 * <UL>
 *  <LI>&lt;"key", String.class&gt;
 *  <LI>&lt;"value", long.class&gt;
 * </UL>
 * <br/>
 * that is, the name of the first element of the map is “key” and its type is “String”, while the name
 * of the second element of the map is “value” and its type is “long”.
 * 
 * The same is for a <b>"END_TIME"</b> event.<br/><br/>
 * 
 * An <b>"AVGRT_EVENT"</b> event is defined by this map:
 * <UL>
 *  <LI>&lt;"timestamp", long.class&gt;
 *  <LI>&lt;"avg", double.class&gt;
 *  <LI>&lt;"stddev", long.class&gt;
 * </UL>
 * <br/>
 * that is, the name of the first element of the map is “timestamp” and its type is “long”, the name
 * of the second element of the map is “avg” and its type is “double” and the name of the third element 
 * of the map is “stddev” and its type is “long”.<br/><br/>
 * 
 * For a more detailed presentation of these concepts, see the provided <a href="">tutorials </a>section of the ECoWare documentation.
 *
 */
public class AvgRTCalculator extends StandardKPICalculator {

	/**
	 * Constructs a new AvgRTCalculator using the specified XML element. The bus server name (that is 
	 * the hostname on which the bus server is running) and Esper configuration are also required.
	 * @param xmlElement the XML element (node) of the configuration file from which retrieve the information to build the KPI object
	 * @param busServer the hostname on which the bus server is running
	 * @param esperConfiguration the Esper current configuration (that is an Configuration object. For further detail see the <a href="http://esper.codehaus.org/" target="_blank">Esper</a> documentation).
	 */
	public AvgRTCalculator(Element xmlElement, String busServer, Configuration esperConfiguration) {

		//XML element parsing
		super(xmlElement, busServer, esperConfiguration);
	}

	@Override
	/**
	 * This method actually starts the "AvgRTCalculator" KPI processing.<br/>
	 */
	public void launch() {
		
		System.out.println("---");
        System.out.println("Average Response Time");
        System.out.println("Initializing calculator...");
		
        //ESPER configuration
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(getEsperConfiguration());

        //map for StartTime and EndTime events
		Map<String, Object> timeEventMap = new HashMap<String, Object>();
		
		/*timeEventMap.put("originID", String.class);
		timeEventMap.put("processNumber", int.class);
		timeEventMap.put("timestamp", long.class);*/
		timeEventMap.put("key", String.class);
		timeEventMap.put("value", long.class);
		
		epService.getEPAdministrator().getConfiguration().addEventType(ECoWareEventType.START_TIME.getValue(), timeEventMap);
		epService.getEPAdministrator().getConfiguration().addEventType(ECoWareEventType.END_TIME.getValue(), timeEventMap);

		//KPI map for filters
	    Map<String, Object> filterMap = new HashMap<String, Object>();

	    //filterMap.put("originID", String.class);
	    filterMap.put("timestamp", long.class);
	    filterMap.put("avg", double.class);
	    filterMap.put("stddev", long.class);

		epService.getEPAdministrator().getConfiguration().addEventType(ECoWareEventType.AVGRT_EVENT.getValue(), filterMap);

		//EPL creation
		//ESPER statement generation
		String esperStatement = "SELECT AVG(et.value - st.value) AS avg, stddev(et.value - st.value) as stddev, current_timestamp() AS timestamp " +
			"FROM StartTime.win:time(" + getIntervalValue() + " " + getIntervalUnit() + ") AS st, " +
				"EndTime.win:time(" + getIntervalValue() + " " + getIntervalUnit() + ") AS et " +
			"WHERE st.key like et.key " +
			"OUTPUT SNAPSHOT EVERY " + getOutputValue() + " " + getOutputUnit();

		EPStatement eplStatement = epService.getEPAdministrator().createEPL(esperStatement);
		
		//listener linking
		eplStatement.addListener(new KPIEventListener(ECoWareEventType.AVGRT_EVENT.getValue(), getPublicationID(), getBusServer()));

    	System.out.println("Calculator initialized");
	}
}