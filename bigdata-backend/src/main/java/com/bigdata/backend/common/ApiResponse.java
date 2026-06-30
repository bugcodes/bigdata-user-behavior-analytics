package com.bigdata.backend.common;

/**
 * 统一接口响应体，负责包装业务数据与错误信息。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public record ApiResponse<T>(String code, String msg, T data) {

    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>("0", "success", data);
    }
}
