package ecoware.tutorial;

import ecoware.ecowareprocessor.ECoWareProcessor;


/**
 * @author Armando Varriale
 * <br/><br/>
 * The custom calculator launcher for the tutorial.
 */
public class CustomKpiLauncher {

	public static void main(String[] args) {
		ECoWareProcessor processor;
		try {
			processor = new ECoWareProcessor(CustomKpiLauncher.class.getResourceAsStream("CustomCalculator.xml"));
			processor.start();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
