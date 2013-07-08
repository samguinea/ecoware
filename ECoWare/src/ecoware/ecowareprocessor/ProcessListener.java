package ecoware.ecowareprocessor;

import java.util.HashMap;

import com.espertech.esper.client.EPServiceProvider;

import ecoware.ecowareaccessmanager.ECoWareEventType;
import ecoware.ecowareaccessmanager.ECoWareMessage;
import ecoware.ecowareaccessmanager.ECoWareMessageListener;

/**
 * 
 * @author Armando Varriale
 * <br/><br/>
 * This class is an implementation of the ECoWareMessageListener interface. In particular, its "notify" method
 * simply forward the message notified to the Eper Provider for the subsequent processing phases.
 */
public class ProcessListener implements ECoWareMessageListener {
	private EPServiceProvider esperSP;
	
	/**
	 * Constructs a new ProcessListener with the specified Esper Service Provider.
	 * @param esperServiceProvider the Esper Service Provider
	 */
	public ProcessListener(EPServiceProvider esperServiceProvider) {
		this.esperSP = esperServiceProvider;
	}

	@SuppressWarnings("unchecked")
	@Override
	/**
	 * Offer an implementation for the messages notification method required by the ECoWareMessageListener interface. 
	 * In particular, this method send the notified message (named here 'event') to the Eper Provider for the 
	 * subsequent processing phases.
	 */
	public void notify(ECoWareMessage event) {
		HashMap<String, Object> esperEvent = new HashMap<String, Object>();
		esperEvent = (HashMap<String, Object>) event.getMessageBody();
		ECoWareEventType eventT = event.getEventType();
		String eventType;
		if(eventT != ECoWareEventType.CUSTOM_EVENT)
			eventType = event.getEventType().getValue();
		else
			eventType = event.getEventName();
		//System.out.println("Event type: " + eventType);
		esperSP.getEPRuntime().sendEvent(esperEvent, eventType);
	}

}
