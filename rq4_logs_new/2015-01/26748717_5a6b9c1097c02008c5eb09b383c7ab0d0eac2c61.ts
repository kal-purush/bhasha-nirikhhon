/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// <reference path="../typings/node/node.d.ts" />
/// <reference path="../typings/lodash/lodash.d.ts" />

// 入力

import _ = require('lodash');

import talk = require('./talk');

/**
 * 会話の入力の種類を表す
 */
export enum InputType {
    /**
     * 数値
     */
    Number = 1,

    /**
     * 文字列
     */
    String = 2,

    /**
     * 特殊入力
     */
    Special = 3
};

/**
 * ユーザーの入力を表すクラス
 */
export class Input {
    /**
     * 入力タイプ
     */
    private inputType: InputType;

    /**
     * 入力後の会話
     */
    private next: talk.Talk;

    public constructor(
        public id: string,
        inputType: InputType,
        next: talk.Talk = null) {
        this.inputType = inputType;
        this.next = next;
    }
}