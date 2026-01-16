describe("Tests that AppController has correct properties", function(){
    it("has tabs", function(){
        var model = new AppController();
        
        expect(model.tabs).toBeDefined();
        expect(typeof model.tabs).toEqual("function");
    });
});

describe("Tests AppController.addTab()", function(){
    var model: AppController;
    
    beforeEach(function(){
        model = new AppController();
    });
    
    it("tab not defined", function(){
        var tab: Tab = new Tab("id", "prettyName");
        
        model.addTab(tab);
        
        expect(model.tabs()[0]).toEqual(tab);
    });
    
    it("tab already defined", function(){
        var tab: Tab = new Tab("id", "prettyName");
        
        model.addTab(tab);
        model.addTab(tab);

        expect(model.tabs()[0]).toEqual(tab);
        expect(model.tabs().length).toEqual(1);
    });
});

describe("Tests AppController.getTabById()", function(){
    var model: AppController;
    var tab1: Tab;
    var tab2: Tab;
    
    beforeEach(function(){
        model = new AppController();
        
        tab1 = new Tab("tab1", "prettyName");
        tab2 = new Tab("tab2", "prettyName");
        
        model.addTab(tab1);
        model.addTab(tab2);
    });
    
    it("has good tabId", function(){
        expect(model.getTabById(tab1.id())).toEqual(tab1);
        expect(model.getTabById(tab1.id())).not.toEqual(tab2);
    });
    
    it("has bad tabId", function(){
        expect(model.getTabById("does not exist")).toBeUndefined();
    });
});

describe("Tests AppController.shouldShowTab()", function(){
    var model: AppController;
    var tab1: Tab;
    var tab2: Tab;

    beforeEach(function(){
        model = new AppController();

        tab1 = new Tab("tab1", "prettyName", true);
        tab2 = new Tab("tab2", "prettyName");

        model.addTab(tab1);
        model.addTab(tab2);
    });
    
    it("has good tab id", function(){
        expect(model.shouldShowTab(tab1.id())).toEqual(true);
        expect(model.shouldShowTab(tab2.id())).toEqual(false);
    });
    
    it("has bad tab id", function(){
        expect(model.shouldShowTab("doesNotExist")).toEqual(false);
    });
});

describe("Tests AppController.switchTab()", function(){
    var model: AppController;
    var tab1: Tab;
    var tab2: Tab;

    beforeEach(function(){
        model = new AppController();

        tab1 = new Tab("tab1", "prettyName", true);
        tab2 = new Tab("tab2", "prettyName");

        model.addTab(tab1);
        model.addTab(tab2);
    });

    it("has good tab id", function(){
        model.switchTab(tab2.id());

        expect(model.shouldShowTab(tab2.id())).toEqual(true);
        expect(model.shouldShowTab(tab1.id())).toEqual(false);
    });

    it("has bad tab id", function(){
        model.switchTab("doesNotExist");

        expect(model.shouldShowTab(tab1.id())).toEqual(true);
        expect(model.shouldShowTab(tab2.id())).toEqual(false);
    });
});