package org.icatproject.ids.client;

/**
 * This is thrown when there is not sufficient physical storage or you have exceeded some quota.
 */
@SuppressWarnings("serial")
public class InsufficientStorageException extends IdsException {

	public InsufficientStorageException(String msg) {
		super(msg);
	}

}
