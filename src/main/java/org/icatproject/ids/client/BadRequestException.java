package org.icatproject.ids.client;

/**
 * Thrown for any kind of bad input parameter
 */
@SuppressWarnings("serial")
public class BadRequestException extends IdsException {

	public BadRequestException(String msg) {
		super(msg);
	}

}
