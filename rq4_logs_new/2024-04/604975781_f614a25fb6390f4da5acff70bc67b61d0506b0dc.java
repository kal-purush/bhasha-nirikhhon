package com.practice.bootintegrate.json;

import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.Expression;

public class SpelDemo {
    public static void main(String[] args) {
        // 创建一个表达式解析器
        ExpressionParser parser = new SpelExpressionParser();

        // 解析表达式
        Expression exp = parser.parseExpression("'Hello World'.concat('!')");

        // 计算表达式的值
        String message = (String) exp.getValue();
        System.out.println(message); // 输出: Hello World!
    }
}