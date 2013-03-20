package org.icatproject.idsclient.exceptions;

@SuppressWarnings("serial")
public class BadRequestException extends IDSException
{
    public BadRequestException(String message)
    {    
        super(message);
    }
}
