package com.cn.exception;

public class MessageFailException extends RuntimeException{
    public MessageFailException(String message) {
        super(message);
    }

    @Override
    public String getMessage() {
        String message = "保单处理失败！请核实保单唯一编号进行重新处理。";
        return super.getMessage() + ":" + message;
    }
}
