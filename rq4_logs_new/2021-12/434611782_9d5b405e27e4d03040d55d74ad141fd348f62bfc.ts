import {
  quad, namedNode, variable, blankNode, literal, defaultGraph,
} from '@rdfjs/data-model';
import { BaseQuad } from '@rdfjs/types';
import {
  termAsQuad, asGraph, asQuadSubject, asQuadPredicate, asQuadObject, asQuad,
} from '../lib';

const NamedNodesQuadWithGraph = quad(namedNode('s'), namedNode('p'), namedNode('o'), namedNode('g'));
const NamedNodesQuad = quad(namedNode('s'), namedNode('p'), namedNode('o'));
const NestedQuads = quad(NamedNodesQuad, namedNode('p'), NamedNodesQuadWithGraph);
const NestedQuadsWithGraph = quad(NamedNodesQuad, namedNode('p'), NamedNodesQuadWithGraph, namedNode('g'));

const BlankPredicate = quad<BaseQuad>(namedNode('s'), blankNode('p'), namedNode('o'), namedNode('g'));
const QuadPredicate = quad<BaseQuad>(namedNode('s'), NamedNodesQuad, namedNode('o'), namedNode('g'));

const NestedInvalidSubject = quad<BaseQuad>(BlankPredicate, namedNode('p'), namedNode('o'));
const NestedInvalidPredicate = quad<BaseQuad>(namedNode('s'), BlankPredicate, namedNode('o'));
const NestedInvalidObject = quad<BaseQuad>(namedNode('s'), namedNode('p'), BlankPredicate);

const NestedInvalidSubject2 = quad<BaseQuad>(BlankPredicate, namedNode('p'), namedNode('o'));
const NestedInvalidPredicate2 = quad<BaseQuad>(namedNode('s'), BlankPredicate, namedNode('o'));
const NestedInvalidObject2 = quad<BaseQuad>(namedNode('s'), namedNode('p'), BlankPredicate);

describe('asQuadSubject checks', () => {
  it('Should cast valid subjects', () => {
    expect(asQuadSubject(namedNode('s')).equals(namedNode('s'))).toBe(true);
    expect(asQuadSubject(blankNode('s')).equals(blankNode('s'))).toBe(true);
    expect(asQuadSubject(variable('s')).equals(variable('s'))).toBe(true);
    expect(asQuadSubject(NamedNodesQuadWithGraph).equals(NamedNodesQuadWithGraph)).toBe(true);
    expect(asQuadSubject(NestedQuads).equals(NestedQuads)).toBe(true);
  });

  it('Should error on invalid subjects', () => {
    expect(() => asQuadSubject(literal('s'))).toThrow();
    expect(() => asQuadSubject(NestedInvalidObject2)).toThrow();
    expect(() => asQuadSubject(defaultGraph())).toThrow();
  });
});

describe('asQuadPredicate checks', () => {
  it('Should cast valid predicates', () => {
    expect(asQuadPredicate(namedNode('p')).equals(namedNode('p'))).toBe(true);
    expect(asQuadPredicate(variable('p')).equals(variable('p'))).toBe(true);
  });

  it('Should error on invalid predicates', () => {
    expect(() => asQuadPredicate(literal('p'))).toThrow();
    expect(() => asQuadPredicate(NestedInvalidObject2)).toThrow();
    expect(() => asQuadPredicate(defaultGraph())).toThrow();
    expect(() => asQuadPredicate(blankNode('p'))).toThrow();
    expect(() => asQuadPredicate(NamedNodesQuadWithGraph)).toThrow();
    expect(() => asQuadPredicate(NestedQuads)).toThrow();
  });
});

describe('asQuadObject checks', () => {
  it('Should cast valid object', () => {
    expect(asQuadObject(namedNode('o')).equals(namedNode('o'))).toBe(true);
    expect(asQuadObject(variable('o')).equals(variable('o'))).toBe(true);
    expect(asQuadObject(blankNode('o')).equals(blankNode('o'))).toBe(true);
    expect(asQuadObject(NamedNodesQuadWithGraph).equals(NamedNodesQuadWithGraph)).toBe(true);
    expect(asQuadObject(NestedQuads).equals(NestedQuads)).toBe(true);
    expect(asQuadObject(literal('o')).equals(literal('o'))).toBe(true);
  });

  it('Should error on invalid object', () => {
    expect(() => asQuadObject(NestedInvalidObject2)).toThrow();
    expect(() => asQuadObject(defaultGraph())).toThrow();
  });
});

describe('asGraph checks', () => {
  it('Should cast valid graphs', () => {
    expect(asGraph(namedNode('g')).equals(namedNode('g'))).toBe(true);
    expect(asGraph(blankNode('g')).equals(blankNode('g'))).toBe(true);
    expect(asGraph(variable('g')).equals(variable('g'))).toBe(true);
    expect(asGraph(defaultGraph()).equals(defaultGraph())).toBe(true);
  });

  it('Should error on invalid graphs', () => {
    expect(() => asGraph(literal('g'))).toThrow();
    expect(() => asGraph(NamedNodesQuadWithGraph)).toThrow();
    expect(() => asGraph(QuadPredicate)).toThrow();
    expect(() => asGraph(NestedInvalidObject2)).toThrow();
  });
});

describe('termAsQuad checks', () => {
  it('Should cast valid quads', () => {
    expect(termAsQuad(NamedNodesQuadWithGraph).equals(NamedNodesQuadWithGraph)).toBe(true);
    expect(termAsQuad(NamedNodesQuad).equals(NamedNodesQuad)).toBe(true);
    expect(termAsQuad(NestedQuads).equals(NestedQuads)).toBe(true);
    expect(termAsQuad(NestedQuadsWithGraph).equals(NestedQuadsWithGraph)).toBe(true);
  });

  it('Should error on invalid quads', () => {
    expect(() => termAsQuad(BlankPredicate)).toThrow();
    expect(() => termAsQuad(NestedInvalidSubject)).toThrow();
    expect(() => termAsQuad(NestedInvalidPredicate)).toThrow();
    expect(() => termAsQuad(NestedInvalidObject)).toThrow();
    expect(() => termAsQuad(QuadPredicate)).toThrow();
    expect(() => termAsQuad(NestedInvalidSubject2)).toThrow();
    expect(() => termAsQuad(NestedInvalidPredicate2)).toThrow();
    expect(() => termAsQuad(NestedInvalidObject2)).toThrow();
  });
});

describe('asQuad checks', () => {
  it('Should cast valid quads', () => {
    expect(asQuad(NamedNodesQuadWithGraph).equals(NamedNodesQuadWithGraph)).toBe(true);
    expect(asQuad(NamedNodesQuad).equals(NamedNodesQuad)).toBe(true);
    expect(asQuad(NestedQuads).equals(NestedQuads)).toBe(true);
    expect(asQuad(NestedQuadsWithGraph).equals(NestedQuadsWithGraph)).toBe(true);
    expect(asQuad(NestedQuadsWithGraph).equals(NestedQuadsWithGraph)).toBe(true);
  });

  it('Should error on invalid quads', () => {
    expect(() => asQuad(BlankPredicate)).toThrow();
    expect(() => asQuad(NestedInvalidSubject)).toThrow();
    expect(() => asQuad(NestedInvalidPredicate)).toThrow();
    expect(() => asQuad(NestedInvalidObject)).toThrow();
    expect(() => asQuad(QuadPredicate)).toThrow();
    expect(() => asQuad(NestedInvalidSubject2)).toThrow();
    expect(() => asQuad(NestedInvalidPredicate2)).toThrow();
    expect(() => asQuad(NestedInvalidObject2)).toThrow();
  });
});