package org.icatproject.ids.client;

/**
 * This shows some kind of failure in the server or in communicating with the server.
 */
@SuppressWarnings("serial")
public class InternalException extends IdsException {

	public InternalException(String msg) {
		super(msg);
	}

}
