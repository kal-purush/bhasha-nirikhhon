import express, { Express, Request, Response, NextFunction } from "express";
import jwt, { JsonWebTokenError } from 'jsonwebtoken';
type JWTUser = {
    email: string;
};

// 인증 미들웨어 로직 (로그인 된 회원이 접근 할 수 있는 엔드포인트에 적용)
export function checkAuthenticated(req: Request, res: Response, next: NextFunction) {
    const token: string | undefined = req.cookies.jwt;  // 쿠키에서 토큰을 가져옴

    if (token == null) return res.redirect('/signin'); //토큰이 없으면(인증된 회원이 아니면) 로그인 페이지로 이동

    //토큰 유효성 검증
    jwt.verify(token, process.env.REFRESH_TOKEN_SECRET ?? '', (err: JsonWebTokenError | null, user: JWTUser) => {
        if (err) {
            console.error(err);
            return res.redirect('/signin'); //토큰 검증 실패하면 로그인 페이지로 이동
        }
        req.user = user;
        next();
    });
}
export function checkNotAuthenticated(req: Request, res: Response, next: NextFunction) {
    const token: string | undefined = req.cookies.jwt;

    if (token == null) return next();  // 변경: 토큰이 없으면 다음 미들웨어/라우터로 진행

    jwt.verify(token, process.env.REFRESH_TOKEN_SECRET ?? '', (err: JsonWebTokenError | null, user: JWTUser) => {
        if (err) {
            console.error(err);
            return next();  // 변경: 토큰 검증 실패시 다음 미들웨어/라우터로 진행
        }
        return res.redirect('/');  // 변경: 토큰이 유효하면 메인 페이지로 리다이렉트
    });
}