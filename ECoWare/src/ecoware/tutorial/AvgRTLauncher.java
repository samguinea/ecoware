package ecoware.tutorial;

import ecoware.ecowareprocessor.ECoWareProcessor;


/**
 * @author Armando Varriale
 * <br/><br/>
 * The Average Response Time launcher for the first tutorial.
 */
public class AvgRTLauncher {

	public static void main(String[] args) {
		ECoWareProcessor processor;
		try {
			processor = new ECoWareProcessor(AvgRTLauncher.class.getResourceAsStream("AvgResponseTime.xml"));
			processor.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
