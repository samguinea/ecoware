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
 * This class is an implementation of a <b>"Reliability"</b> calculator (KPI).
 * It extends the <a href="StandardKPICalculator.html">StandardKPICalculator</a> abstract class by 
 * adding some specific properties that characterize a <i>"Reliability"</i> calculator and providing an 
 * adequate implementation of the "launch()" method.<br/><br/>
 * 
 * The properties that have been introduced refer to the concept of "timeout", that is the timeout time within
 * the analysis was realized. The "timeout" is characterized by "timeout unit" (eg. unit of time) and "timeout unit" 
 * (eg. a positive number); so if "timeout unit" is "seconds" and "timeout value" is "10" it means that the 
 * timeout is 10 seconds.<br/><br/>
 * 
 * An <b>"Reliability"</b> calculator is a processor that produce an estimation of the reliability of one or 
 * more specific tasks (services), that are those present in the subscribe keys list. More precisely, it collects two events, 
 * a "START_TIME" event and a "END_TIME" event (that are sent by subscribed services/tasks) and identifies the tasks (services) that 
 * are completed within the specified timeout inside the inspection window (see 
 * <a href="StandardKPICalculator.html">StandardKPICalculator</a> for more details on the inspection window). Finally, 
 * according to the specified output frequency (that is the "output production" parameter, see 
 * <a href="StandardKPICalculator.html">StandardKPICalculator</a> for more details on it), it sends on the bus an 
 * "RELIABILITY_EVENT" event that contains data relative to the estimated reliability in terms of "completed_task/total_task" ratio.<br/><br/>
 * 
 * In ECoWare (and Esper too) an event is modeled as a "HashMap&lt;String, Object&gt;", so its content is a set 
 * of "&lt;key, value&gt;" pairs. Each event has its specific map that is required to make possible their correct usage 
 * during analysis processes. <br/><br/>
 * 
 * As said, the <i>"Reliability"</i> calculator requires in input </i>"START_TIME"</i> and </i>"END_TIME"</i> 
 * events (the start and the end time related to a certain task/service that you want monitor/analyze) while 
 * produces in output an </i>"RELIABILITY_EVENT"</i> event with the estimated reliability.<br/><br/>
 * 
 * For a <i>"Reliability"</i> calculator, a <b>"START_TIME"</b> event is defined by this map:
 * <UL>
 *  <LI>&lt;"key", String.class&gt;
 *  <LI>&lt;"timestamp", long.class&gt;
 * </UL>
 * <br/>
 * that is, the name of the first element of the map is "key" and its type is "String", while the name
 * of the second element of the map is "timestamp" and its type is "long".
 * 
 * The same is for a <b>"END_TIME"</b> event.<br/><br/>
 * 
 * A <b>"RELIABILITY_EVENT"</b> event is defined by this map:
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
 */
public class ReliabilityCalculator extends StandardKPICalculator {
	private String timeoutUnit;
	private double timeoutValue;
	
	/**
	 * Constructs a new ReliabilityCalculator using the specified XML element. The bus server name (that is 
	 * the hostname on which the bus server is running) and Esper configuration are also required.
	 * @param xmlElement the XML element (node) of the configuration file from which retrieve the information to build the KPI object
	 * @param busServer the hostname on which the bus server is running
	 * @param esperConfiguration the Esper current configuration (that is an Configuration object. For further detail see the <a href="http://esper.codehaus.org/" target="_blank">Esper</a> documentation).
	 */
	public ReliabilityCalculator(Element xmlElement, String busServer, Configuration esperConfiguration) {
		super(xmlElement, busServer, esperConfiguration);
		
		timeoutUnit = xmlElement.getElementsByTagName("timeoutUnit").item(0).getTextContent();
		timeoutValue = Integer.parseInt(xmlElement.getElementsByTagName("timeoutValue").item(0).getTextContent());
	}

	@Override
	/**
	 * This method actually starts the "ReliabilityCalculator" KPI processing.<br/>
	 */
	public void launch() {
		Logger.logInfo("---");
		Logger.logInfo("Reliability");
		Logger.logInfo("Initializing calculator...");
		
        //ESPER configuration
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(getEsperConfiguration());

        //map for StartTime and EndTime events
		Map<String, Object> timeEventMap = new HashMap<String, Object>();
		timeEventMap.put("key", String.class);
		timeEventMap.put("timestamp", long.class);
		
		epService.getEPAdministrator().getConfiguration().addEventType(ECoWareEventType.START_TIME.getValue(), timeEventMap);
		epService.getEPAdministrator().getConfiguration().addEventType(ECoWareEventType.END_TIME.getValue(), timeEventMap);

		//KPI map for filters
	    Map<String, Object> filterMap = new HashMap<String, Object>();
		filterMap.put("timestamp", long.class);
	    filterMap.put("value", double.class);
	    filterMap.put("source", String.class);

		epService.getEPAdministrator().getConfiguration().addEventType(ECoWareEventType.RELIABILITY_EVENT.getValue(), filterMap);

		//EPL creation
		//ESPER statements generation
		String esperStatement = "CREATE WINDOW TotalTxs.win:length(1) " +
			"(total long)";

		epService.getEPAdministrator().createEPL(esperStatement);

		esperStatement = "INSERT INTO TotalTxs " +
			"SELECT COUNT(*) AS total " +
			"FROM StartTime.win:time(" + getIntervalValue() + " " + getIntervalUnit() + ")";

		epService.getEPAdministrator().createEPL(esperStatement);

		esperStatement = "CREATE WINDOW CompletedTxs.win:length(1) " +
			"(completed long)";

		epService.getEPAdministrator().createEPL(esperStatement);

		esperStatement = "INSERT INTO CompletedTxs " +
			"SELECT COUNT(*) AS completed " +
			"FROM StartTime.win:time(" + getIntervalValue() + " " + getIntervalUnit() + ") AS st, " +
				"EndTime.win:time(" + getIntervalValue() + " " + getIntervalUnit() + ") AS et " +
			"WHERE st.key like et.key AND (et.timestamp - st.timestamp) < " + timeoutValue + " " + timeoutUnit;

		epService.getEPAdministrator().createEPL(esperStatement);

		esperStatement = "SELECT (completed / total) AS value, current_timestamp() AS timestamp, '"+getPublicationID()+"' as source " +
			"FROM TotalTxs, " +
				"CompletedTxs " +
			"OUTPUT SNAPSHOT EVERY " + getOutputValue() + " " + getOutputUnit();
		
		EPStatement eplStatement = epService.getEPAdministrator().createEPL(esperStatement);
		eplStatement.addListener(new KPIEventListener(ECoWareEventType.RELIABILITY_EVENT.getValue(), getPublicationID(), getBusServer()));
		Logger.logInfo("Calculator initialized");
	}
}