export const createEntity = (type) => {
    return {
        type: type,
        id: Math.floor(Math.random() * 100),
    }
};
export const createContainer = (direction, postion, type) => {
    return {
        direction: direction,
        postion: postion,
        entitys: [],
        type: type
    };
};

export function createGame() {
    return {
        isGameOver: false,
        setSubscribe: function (cn) {
            this.subscribe = cn;
        },
        subscribe: function () {
            console.log("this is old subscribe");
        },
        removeEntity: function (containerObj, id) {
            containerObj.entitys = containerObj.entitys.filter((item) => { return item.id !== id });
            this.subscribe();
        },

        addEntity: function (containerObj, item) {
            containerObj.entitys.push(item)
            this.subscribe();
        },

        moveBoatContainer: function (containerObj, mountainObject) {
            if (containerObj.direction == 'right') {
                containerObj.direction = 'left';
            } else {
                containerObj.direction = 'right';
            }
            this.checkBoatStatus(mountainObject);
        },

        checkBoatStatus: function (mountainObject) {
            let personLength = mountainObject.entitys.filter((e) => { return e.type === 'person' })?.length
            let giantLength = mountainObject.entitys.filter((e) => { return e.type === 'giant' })?.length;
            if (personLength === 0) {
                return;
            };
            if (personLength < giantLength) {
                this.isGameOver = true;
            } else {
                this.isGameOver = false;
            }
            this.subscribe();
        },
    }
}