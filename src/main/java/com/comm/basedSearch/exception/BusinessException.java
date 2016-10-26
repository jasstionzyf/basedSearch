/**
 * 
 */
package com.comm.basedSearch.exception;

/**
 * 业务层受检异常,异常发生时应该在日志中详细记录异常消息
 * 如果异常在发生的时刻已经记录了异常日志,那么上层调用方法就不用重复记录
 * @author Renxc
 *
 */
public class BusinessException extends Exception {

    private static final long serialVersionUID = 1240015972592825169L;
    private boolean logged;
    private int errorCode = 0; //错误代码 默认为0

    /**
     * 业务层受检异常
     * @param errMsg 异常消息
     * @param logged 是否已记录异常
     */
    public BusinessException(String errMsg, boolean logged) {
        super(errMsg);
        this.logged = logged;
    }

    /**
     * 业务层受检异常
     * @param errMsg 异常消息
     * @param logged 是否已记录异常
     * @param errorCode 错误代码
     */

    public BusinessException(int errorCode, String errMsg, boolean logged) {
        super(errMsg);
        this.logged = logged;
        this.errorCode = errorCode;
    }

    /**
     * 业务层受检异常
     * @param errMsg 异常消息
     * @param throwable  异常堆栈
     * @param logged 是否已记录异常
     */
    public BusinessException(String errMsg, Throwable throwable, boolean logged) {
        super(errMsg, throwable);
        this.logged = logged;
    }

    /**
     * 业务层受检异常
     * @param errorCode 错误代码
     * @param errMsg 异常消息
     * @param throwable  异常堆栈
     * @param logged 是否已记录异常
     */
    public BusinessException(int errorCode, String errMsg, Throwable throwable, boolean logged) {
        super(errMsg, throwable);
        this.logged = logged;
        this.errorCode = errorCode;
    }

    public boolean isLogged() {
        return logged;
    }

    public void setLogged(boolean logged) {
        this.logged = logged;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }


}