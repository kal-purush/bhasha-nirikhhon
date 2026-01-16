import Vue from 'vue';
import Component from 'vue-class-component';
import { Watch } from 'vue-property-decorator';
import WithRender from './composant.html?style=./composant.scss';
import { ModulState } from '@/app/store/modul-state';
import { ComposantState } from './composant.state';
import { ModulActions } from '@/app/store/actions';

@WithRender
@Component
export class Composant extends Vue {
    public content: string = '';

    public created() {
        this.getMeta();
    }

    @Watch('$route')
    private getMeta(): void {
        this.$store.dispatch(ModulActions.COMPOSANT_GET, this.$route.meta).then((v) => {
            this.content = this.state.composant.portrait[0].content;
        });
    }

    public get state(): ComposantState {
        return (this.$store.state as ModulState).composantState;
    }
}