/**
 * 
 */
package ecoware.ecowareaccessmanager;

import java.util.EventObject;

/**
 * @author Armando Varriale
 *
 */
@SuppressWarnings("serial")
public class ECoWareMessageEvent extends EventObject {
	private ECoWareMessage message;
	
	/**
	 * 
	 * @param message the event message
	 */
	public ECoWareMessageEvent(ECoWareMessage message) {
		super(message);
		this.message = message;
	}

	/**
	 * @return the message
	 */
	public ECoWareMessage getMessage() {
		return message;
	}
}
