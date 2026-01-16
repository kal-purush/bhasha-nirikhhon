import Vue = require("vue");
import VueRouter = require("vue-router");
import ComponentOptions = Vue.ComponentOptions;
import { Car } from "../data/Car";

interface ICarCardComponent extends Vue {
    car: Car;
}
export function getCardCardComponent() {
    return Vue.component("CarCardComponent", {
        template: `
        <div class="col s8 m6">
            <div class="card darken-4" v-bind:class="[car.IsCollected() ? 'green' : 'red']">
                <div class="card-content white-text">
                    <span class="card-title">{{car.getNameCapitalized()}}</span>
                    <p class="card-text">Plate: {{car.getPlateCapitalized()}}</p>
                </div>
                <div class="card-action darken-3">
                        <a href="javascript:void(0)" class="white-text" v-on:click="ToggleCollected()">{{car.IsCollected() ? "Put back" : "Mark collected"}}</a>
                    </div>
            </div>
        </div>
        `,
        methods: {
            ToggleCollected: function (e) {
                this.car.ToggleCollected();
                this.$forceUpdate();
                this.$emit("update");
            }
        },
        props: ["car"]
        // We need to explicitly annotate the exported options object
        // with the CarCardComponent type
    } as ComponentOptions<ICarCardComponent>);
}