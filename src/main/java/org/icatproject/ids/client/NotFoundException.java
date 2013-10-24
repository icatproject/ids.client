package org.icatproject.ids.client;

/**
 * Thrown when the requested data do not exist
 */
@SuppressWarnings("serial")
public class NotFoundException extends IdsException {

	public NotFoundException(String msg) {
		super(msg);
	}

}
