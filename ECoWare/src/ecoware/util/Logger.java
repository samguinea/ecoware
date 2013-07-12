package ecoware.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author Armando Varriale
 * <br/><br/>
 * This class is the ECoWare logger.
 *
 */

public class Logger {
	private static Log log = LogFactory.getLog(Logger.class);
	
	/**
	 * Submit a debug message to the logger.
	 * @param message the message to log
	 */
	public static void logDebug(String message){
		log.debug(message);
	}
	
	/**
	 * Submit an info message to the logger.
	 * @param message the message to log
	 */
	public static void logInfo(String message){
		log.info(message);
	}
	
	/**
	 * Submit an error message to the logger.
	 * @param message the message to log
	 */
	public static void logError(String message){
		log.error(message);
	}
}
