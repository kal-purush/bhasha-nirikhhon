import React from 'react';

export interface ModalData {
  body: React.ReactNode;
  title?: string;
  visibility?: boolean;
}

export default class Modal {
  public readonly body!: React.ReactNode;
  public readonly title!: string;
  public visibility!: boolean;

  private _update!: (values: Partial<Modal>) => void;

  constructor({ title, body, visibility }: ModalData) {
    this.body = body;
    if (title) {
      this.title = title;
    }
    if (visibility) {
      this.visibility = visibility;
    }
  }

  public get update() {
    return this._update;
  }
  public set update(fn: (values: Partial<ModalData>) => any) {
    this._update = fn;
  }

  private toggle(visibility: boolean) {
    this.visibility = visibility;
    this.update({ visibility: this.visibility });
  }

  public open() {
    this.toggle(true);
  }
  public close() {
    this.toggle(false);
  }
}