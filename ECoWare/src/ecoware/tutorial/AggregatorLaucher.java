package ecoware.tutorial;

import ecoware.ecowareprocessor.ECoWareProcessor;


/**
 * @author Armando Varriale
 * <br/><br/>
 * The Aggregator launcher for the fourth tutorial.
 */
public class AggregatorLaucher {

	public static void main(String[] args) {
		ECoWareProcessor processor;
		try {
			processor = new ECoWareProcessor(AggregatorLaucher.class.getResourceAsStream("Aggregator.xml"));
			processor.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
