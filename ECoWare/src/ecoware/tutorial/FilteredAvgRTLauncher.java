package ecoware.tutorial;

import ecoware.ecowareprocessor.ECoWareProcessor;


/**
 * @author Armando Varriale
 * <br/><br/>
 * The filtered Average Response Time launcher for the second tutorial.
 */
public class FilteredAvgRTLauncher {

	public static void main(String[] args) {
		ECoWareProcessor processor;
		try {
			processor = new ECoWareProcessor(FilteredAvgRTLauncher.class.getResourceAsStream("FilteredAvgRT.xml"));
			processor.start();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
