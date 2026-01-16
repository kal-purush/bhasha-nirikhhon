import {event, prop, watch, Component, Filter, Mixin, Partial} from "./decorators"
declare var expect: Chai.ExpectStatic

describe('decorators', function() {

  beforeEach(function() {
    this.Test = class {testMethod(){}testProperty = true}
    this.test = new this.Test();
  })

  describe('event decorator', function () {

    it('eventプロパティがあればそこに追加する', function() {
      this.Test.events = {}
      event('event:test')(this.Test.prototype, 'testMethod')

      expect(this.Test.events).to.have.property('event:test')
    })

    it('eventプロパティがない時は追加する', function() {
      event('event:test')(this.Test.prototype, 'testMethod')

      expect(this.Test).to.have.property('events')
      expect(this.Test.events).to.have.property('event:test')
    })

  })

  describe('prop decorator', function() {

    before(function() {
      this.PropOption = {
        type: Number,
        default: 1,
        required: false,
        twoWay: false,
        validator: () => true
      }
    })

    it('propsプロパティがある時はそこに追加する', function() {
      this.PropOption.props = {}
      prop(this.PropOption)(this.Test.prototype, 'testProperty')

      expect(this.Test.props).to.have.property('testProperty')
      expect(this.Test.props.testProperty).to.equal(this.PropOption)
    })

    it('propsプロパティがない時は追加する', function() {
      prop(this.PropOption)(this.Test.prototype, 'testProperty')

      expect(this.Test).to.have.property('props')
      expect(this.Test.props).to.have.property('testProperty')
      expect(this.Test.props.testProperty).to.equal(this.PropOption)
    })

  })

  describe('watch decorator', function() {

    before(function() {
      this.onChange = (value: boolean, oldValue: boolean) => {}
    })

    it('watchプロパティがある時はそこに追加する', function() {
      this.Test.watch = {}
      watch(this.onChange)(this.Test.prototype, 'testProperty')

      expect(this.Test.watch).to.have.property('testProperty')
      expect(this.Test.watch.testProperty).to.equal(this.onChange)
    })

    it('watchプロパティがない時は追加する', function() {
      watch(this.onChange)(this.Test.prototype, 'testProperty')

      expect(this.Test).to.have.property('watch')
      expect(this.Test.watch).to.have.property('testProperty')
      expect(this.Test.watch.testProperty).to.equal(this.onChange)
    })

  })

  describe('Component decorator', function() {

    before(function() {
      this.Component = {}
    })

    it('componentsプロパティがある時はそこに追加する', function() {
      this.Test.components = {}
      Component('component', this.Component)(this.Test)

      expect(this.Test.components.component).to.equal(this.Component)
    })

    it('componentsプロパティがない時は追加する', function() {
      Component('component', this.Component)(this.Test)

      expect(this.Test).have.property('components')
      expect(this.Test.components.component).to.equal(this.Component)
    })

  })

  describe('Filter decorator', function() {

    before(function() {
      this.testFilter = () => {}
    })

    it('filterプロパティがある時はそこに追加する', function() {
      this.Test.filters = {}
      Filter('testFilter', this.testFilter)(this.Test)

      expect(this.Test.filters).to.have.property('testFilter')
      expect(this.Test.filters.testFilter).to.equal(this.testFilter)
    })

    it('filterプロパティがない時は追加する', function() {
      Filter('testFilter', this.testFilter)(this.Test)

      expect(this.Test).to.have.property('filters')
      expect(this.Test.filters).to.have.property('testFilter')
      expect(this.Test.filters.testFilter).to.equal(this.testFilter)
    })

  })

  describe('Mixin decorator', function() {

    before(function() {
      this.testMixin = {}
    })

    it('mixinsプロパティがある時はそこに追加する', function() {
      this.Test.mixins = []
      Mixin(this.testMixin)(this.Test)

      expect(this.Test.mixins).to.include(this.testMixin)
    })

    it('mixinsプロパティがない時は追加する', function() {
      Mixin(this.testMixin)(this.Test)

      expect(this.Test).to.have.property('mixins')
      expect(this.Test.mixins).to.be.instanceof(Array)
      expect(this.Test.mixins).to.include(this.testMixin)
    })

  })

  describe('Parital decorator', function() {

    before(function() {
      this.testPartial = "<div></div>"
    })

    it('partialsプロパティがある時はそこに追加する', function() {
      this.Test.partials = {}
      Partial('testPartial', this.testPartial)(this.Test)

      expect(this.Test.partials).to.have.property('testPartial')
      expect(this.Test.partials.testPartial).to.equal(this.testPartial)
    })

    it('partialsプロパティがない時は追加する', function() {
      Partial('testPartial', this.testPartial)(this.Test)

      expect(this.Test).to.have.property('partials')
      expect(this.Test.partials).to.have.property('testPartial')
      expect(this.Test.partials.testPartial).to.equal(this.testPartial)
    })

  })

})