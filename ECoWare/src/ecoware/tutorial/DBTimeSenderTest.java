package ecoware.tutorial;

import java.io.IOException;
import java.util.HashMap;
import ecoware.ecowareaccessmanager.ECoWareEventType;
import ecoware.ecowareaccessmanager.ECoWareMessageSender;

public class DBTimeSenderTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Producer started...");
		HashMap<String, Object> mapMsg = new HashMap<String, Object>();
		ECoWareMessageSender sender;
		try {
			sender = new ECoWareMessageSender("localhost", "DBSystem");
			sender.startConnection();
			
			while(true){

				mapMsg.put("key", "105");
				mapMsg.put("value", 1.0);
				mapMsg.put("instanceId", "test");
				sender.send(mapMsg, ECoWareEventType.START_DBTIME, -1);
				
				mapMsg.put("key", "105");
				mapMsg.put("value", 3.0);
				mapMsg.put("instanceId", "test");
				sender.send(mapMsg, ECoWareEventType.END_DBTIME, -1);

				mapMsg.put("key", "105");
				mapMsg.put("value", 1.5);
				mapMsg.put("instanceId", "test");
				sender.send(mapMsg, ECoWareEventType.START_DBTIME, -1);
				
				mapMsg.put("key", "105");
				mapMsg.put("value", 2.8);
				mapMsg.put("instanceId", "test");
				sender.send(mapMsg, ECoWareEventType.END_DBTIME, -1);

				mapMsg.put("key", "105");
				mapMsg.put("value", 1.25);
				mapMsg.put("instanceId", "test");
				sender.send(mapMsg, ECoWareEventType.START_DBTIME, -1);
				
				mapMsg.put("key", "105");
				mapMsg.put("value", 3.8);
				mapMsg.put("instanceId", "test");
				sender.send(mapMsg, ECoWareEventType.END_DBTIME, -1);

				mapMsg.put("key", "105");
				mapMsg.put("value", 3.0);
				mapMsg.put("instanceId", "test");
				sender.send(mapMsg, ECoWareEventType.COUNT_DB, -1);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.println("Producer finished...");	
	}
}
