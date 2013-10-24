package org.icatproject.ids.client;

/**
 * Thrown when the requested data are not on line. This can only happen when secondary storage is
 * used.
 */
@SuppressWarnings("serial")
public class DataNotOnlineException extends IdsException {

	public DataNotOnlineException(String msg) {
		super(msg);
	}

}
