/**
 * Created by zhouli on 18/10/30
 */

context('node-blog-admin list post', () => {
    describe('博客页面测试', function() {
        it('跳转博客页面', function() {
            cy.visit('http://localhost:8085/#/list/PostNew')
        })
        it('收缩导航', () => {
            cy.get('.header-toggle-class').click()
        })
        it('展开导航', () => {
            cy.get('.header-toggle-class').click()
        })
        it('收缩导航', () => {
            cy.get('.header-toggle-class').click()
        })
        it('展开导航', () => {
            cy.get('.header-toggle-class').click()
        })
        it('展开博客管理', () => {
            cy.get('.postNavitem').click()
        })
        it('收缩博客管理', () => {
            cy.get('.postNavitem').click()
        })
        it('展开博客管理', () => {
            cy.get('.postNavitem').click()
        })
    })


})