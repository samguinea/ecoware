package ecoware.tutorial;

import java.io.IOException;
import java.util.HashMap;

import ecoware.ecowareaccessmanager.ECoWareEventType;
import ecoware.ecowareaccessmanager.ECoWareMessageSender;

/**
 * @author Armando Varriale
 * <br/><br/>
 * The Sender Test for the third tutorial.
 */
public class FilterSenderTest {

	public static void main(String[] args) {
		
		System.out.println("Producer started...");
		ECoWareMessageSender sender;
		HashMap<String, Object> mapMsg = new HashMap<String, Object>();
//		mapMsg.put("key", "105");
//		mapMsg.put("value", 1.0);
		int msgs = 10000;
		int i = 0;
		
		try {
			sender = new ECoWareMessageSender("localhost", "BrowserInfo");
			sender.startConnection();
			while(i<msgs){
				mapMsg.put("key", "105");
				mapMsg.put("value", 1.0);
				sender.send(mapMsg, ECoWareEventType.START_TIME, -1);
				mapMsg.put("key", "105");
				mapMsg.put("value", 4.0);
				sender.send(mapMsg, ECoWareEventType.END_TIME, -1);
				i++;
			}
			sender.stopConnection();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.println("Producer finished...");	
	}
}
