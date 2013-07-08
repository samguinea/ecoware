package ecoware.tutorial;

import ecoware.ecowareprocessor.ECoWareProcessor;


/**
 * @author Armando Varriale
 * <br/><br/>
 * The Arrival Rate launcher for the third tutorial.
 */
public class ArrivalRateLauncher {

	public static void main(String[] args) {
		ECoWareProcessor processor;
		try {
			processor = new ECoWareProcessor(ArrivalRateLauncher.class.getResourceAsStream("ArrivalRate.xml"));
			processor.start();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
