package org.icatproject.ids.client;

/**
 * Thrown when an attempt is made to use some functionality that is not supported by the
 * implementation.
 */
@SuppressWarnings("serial")
public class NotImplementedException extends IdsException {

	public NotImplementedException(String msg) {
		super(msg);
	}

}
