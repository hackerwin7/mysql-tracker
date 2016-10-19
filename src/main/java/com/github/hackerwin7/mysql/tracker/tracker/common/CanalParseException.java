package com.github.hackerwin7.mysql.tracker.tracker.common;

import org.apache.commons.lang.exception.NestableRuntimeException;

/**
 * Created by hp on 14-9-3.
 */
public class CanalParseException extends NestableRuntimeException {

    private static final long serialVersionUID = -7288830284122672209L;

    public CanalParseException(String errorCode){
        super(errorCode);
    }

    public CanalParseException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public CanalParseException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public CanalParseException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalParseException(Throwable cause){
        super(cause);
    }

    public Throwable fillInStackTrace() {
        return this;
    }

}
