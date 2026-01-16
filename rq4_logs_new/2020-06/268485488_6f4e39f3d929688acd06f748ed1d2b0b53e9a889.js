'use strict'
// import assertion: chai
const chai = require('chai')
var sinon = require('sinon')
chai.use(require('sinon-chai'))

const { expect } = chai

// import sequelize-test-helpers
const {
  sequelize,
  dataTypes,
  checkModelName,
  checkPropertyExists
} = require('sequelize-test-helpers')

// import model
const Course = require('../../models/course')(sequelize, dataTypes)

// create a instance
const course = new Course()

// course model properties
const courseProperties = [
  'name',
  'description',
  'image',
  'introVideo',
  'teacherName',
  'teacherDescrip',
  'totalTime',
  'totalLessons',
  'price',
  'status',
  'submittedDate',
  'intoMarketDate',
  'ratingAverage',
  'ratingCount',
  'studentCount',
  'CourseCategoryId',
  'CourseSubCategoryId',
  'UserId'
]

describe('# Course model', () => {
  before(done => {
    done()
  })

  context('check model name', () => {
    checkModelName(Course)('Course')
  })

  context('check properties', () => {
    courseProperties.forEach(checkPropertyExists(course))
  })

  context('associations', () => {
    const UserEnrollment = 'UserEnrollment'
    const CourseReviewPost = 'CourseReviewPost'
    const Lesson = 'Lesson'
    const Rating = 'Rating'
    const DiscussPost = 'DiscussPost'
    const Assignment = 'Assignment'
    const User = 'User'
    const CourseCategory = 'CourseCategory'
    const CourseSubCategory = 'CourseSubCategory'
    const Order = 'Order'
    const Cart = 'Cart'

    before(() => {
      Course.associate({ UserEnrollment })
      Course.associate({ CourseReviewPost })
      Course.associate({ Lesson })
      Course.associate({ Rating })
      Course.associate({ DiscussPost })
      Course.associate({ Assignment })
      Course.associate({ User })
      Course.associate({ CourseCategory })
      Course.associate({ CourseSubCategory })
      Course.associate({ Order })
      Course.associate({ Cart })
    })

    it('Course has many userEnrollments', (done) => {
      expect(Course.hasMany).to.have.been.calledWith(UserEnrollment)
      done()
    })
    it('Course has many CourseReviewPosts', (done) => {
      expect(Course.hasMany).to.have.been.calledWith(CourseReviewPost)
      done()
    })
    it('Course has many Lessons', (done) => {
      expect(Course.hasMany).to.have.been.calledWith(Lesson)
      done()
    })
    it('Course has many Ratings', (done) => {
      expect(Course.hasMany).to.have.been.calledWith(Rating)
      done()
    })
    it('Course has many DiscussPosts', (done) => {
      expect(Course.hasMany).to.have.been.calledWith(DiscussPost)
      done()
    })
    it('Course has many Assignments', (done) => {
      expect(Course.hasMany).to.have.been.calledWith(Assignment)
      done()
    })
    it('Course belongs to User', (done) => {
      expect(Course.belongsTo).to.have.been.calledWith(User)
      done()
    })
    it('Course belongs to CourseCategories', (done) => {
      expect(Course.belongsTo).to.have.been.calledWith(CourseCategory)
      done()
    })
    it('Course belongs to CourseSubCategories', (done) => {
      expect(Course.belongsTo).to.have.been.calledWith(CourseSubCategory)
      done()
    })
    it('Course belongs to FavoriteUsers', (done) => {
      expect(Course.belongsToMany).to.have.been.calledWith(User)
      done()
    })
    it('Course belongs to Orders', (done) => {
      expect(Course.belongsToMany).to.have.been.calledWith(Order)
      done()
    })
    it('Course belongs to Carts', (done) => {
      expect(Course.belongsToMany).to.have.been.calledWith(Cart)
      done()
    })

  })
})
