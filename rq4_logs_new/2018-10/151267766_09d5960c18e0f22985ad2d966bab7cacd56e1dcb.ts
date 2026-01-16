// Type definitions for @storybook/react 4.0.0-alpha
/// <reference types="webpack-env" />

import * as React from 'react'

import { Options } from '@storybook/addon-info'

declare module '@storybook/react' {
  export interface Story {
    readonly kind: string
    add(
      storyName: string,
      callback: RenderFunction,
      options?: { info: Options & { text?: string | React.ReactNode } }
    ): this
    addDecorator(decorator: StoryDecorator): this
  }

  export function addDecorator(decorator: StoryDecorator): void
  export function configure(fn: () => void, module: NodeModule): void
  export function setAddon(addon: object): void
  export function storiesOf(name: string, module: NodeModule): Story
  export function storiesOf<T>(name: string, module: NodeModule): Story & T
  export function forceReRender(): void

  export interface StoryObject {
    name: string
    render: RenderFunction
  }

  export interface StoryBucket {
    kind: string
    stories: StoryObject[]
  }

  export function getStorybook(): StoryBucket[]
}