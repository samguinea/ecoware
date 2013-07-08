package ecoware.ecowareaccessmanager;


/**
 * @author Armando Varriale
 * <br/><br/>
 * This interface represent the listener to ECoWare messages and must be implemented to create your specific listener for your application. <br/>
 * It provides only one method, "notify", that is executed every time a new message is present in the queue (on which an <a href="ECoWareMessageReceiver.java">ECoWareMessageReceiver</a> is linked to).<br/> 
 * For this reason, you must provide an implementation for the "notify" method that reflect your specific operations to do when a new message arrived.
 *
 */
public interface ECoWareMessageListener {
	
	/**
	 * This method is executed each time a new message arrives in a receiver's queue.
	 * @param event This is the ECoWare message that is arrived.
	 */
	public void notify(ECoWareMessage event);
}