package ecoware.ecowareaccessmanager;

/**
 *   
 * @author Armando Varriale
 * <br/><br/>
 * This is a Java Enum and represent the event types that can be used in ECoWare to perform data aggregation and data analysis.
 *
 */
public enum ECoWareEventType {
START_TIME("StartTime"), END_TIME("EndTime"), FILTER("Filter"), LP_FILTER("LPFilter"), HP_FILTER("HPFilter"), PRIMARY_AGGREGATION("PrimaryAggregation"), SECONDAY_AGGREGATION("SecondaryAggregation"), CUSTOM_EVENT("CustomEvent"),
AVGRT_EVENT("AvgRT"), ARRIVALRATE_EVENT("ArrivalRate"), RELIABILITY_EVENT("Reliability"), AGGREGATOR_EVENT("Aggregator"), DBTIME_EVENT("DBTime"), START_DBTIME("StartDBTime"), END_DBTIME("EndDBTime"), COUNT_DB("CountDB");
	
	private final String value;
	
	private ECoWareEventType(String value){
		this.value = value;
	}
	
	public String getValue(){
		return this.value;
	}
	
	public static ECoWareEventType getEventType(String eventTypeName) {
		for(ECoWareEventType ev: ECoWareEventType.values()){
			if(ev.getValue().equals(eventTypeName))
				return ev;
		}
		return CUSTOM_EVENT;
	}
};
