package org.icatproject.idsclient.exceptions;

@SuppressWarnings("serial")
public class ForbiddenException extends IDSException
{
    public ForbiddenException(String message)
    {    
        super(message);
    }
}

