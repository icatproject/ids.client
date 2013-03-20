package org.icatproject.idsclient.exceptions;

@SuppressWarnings("serial")
public class IDSException extends RuntimeException
{   
    public IDSException(String message)
    {
        super(message);
    }
}