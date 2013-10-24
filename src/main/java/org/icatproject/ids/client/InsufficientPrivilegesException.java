package org.icatproject.ids.client;

/**
 * Thrown when you are denied access to the data. This may be because the session has expired or the
 * authorization rules associated with the owner of the session do not allow the requested access.
 */
@SuppressWarnings("serial")
public class InsufficientPrivilegesException extends IdsException {

	public InsufficientPrivilegesException(String msg) {
		super(msg);
	}

}
