import test from 'ava'

import { Converter, toBinary, toOctal, toDecimal, toHex, fillLength } from '../utils/converter'

const converter = new Converter('86', 16)

test("Converter.toBinary", t => {
  t.true(converter.toBinary() === '10000110')
})

test("Converter.toOctal", t => {
  t.true(converter.toOctal() === '206')
})

test("Converter.toDecimal", t => {
  t.true(converter.toDecimal() === '134')
})

test("Converter.toHex", t => {
  t.true(converter.toHex() === '86')
})

test("Converter.fill", t => {
  t.true(converter.fill(8) === '00000086')
})

test("Function", t => {
  t.pass()
})

test("toBinary", t => {
  t.true(toBinary('86', 16) === '10000110')
})

test("toOctal", t => {
  t.true(toOctal('86', 16) === '206')
})

test("toDecimal", t => {
  t.true(toDecimal('86', 16) === '134')
})

test("toHex", t => {
  t.true(toHex('86', 16) === '86')
})

test("fill", t => {
  t.true(fillLength('86', 8) === '00000086')
})