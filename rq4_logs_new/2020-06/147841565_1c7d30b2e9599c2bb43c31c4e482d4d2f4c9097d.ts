import { VuexModule, Module, Action, Mutation, getModule } from 'vuex-module-decorators'
import Art from "@ctsy/api-sdk/dist/modules/Art";
import { SearchResult, SearchWhere } from '@ctsy/api-sdk/dist/lib'
import hook, { HookWhen } from '@ctsy/hook';
import { delay_cb, array_tree } from 'castle-function';
import { uniq } from 'lodash';
var WaitingUnitIDs: number[] = [];
@Module({})
export default class orgs extends VuexModule {
    Result: SearchResult<any> = new SearchResult();
    Where: SearchWhere = new SearchWhere();
    Tree: any[] = []
    Art: Art.ClassArt = new Art.ClassArt;
    /**
     * 以分类的CID为键的Map
     */
    ClassMap: { [index: string]: Art.ClassArt } = {};
    @Mutation
    set_art_where(data: any) {
        this.Where = data;
    }
    @Mutation
    set_art_result(data: any) {
        this.Result = data;
    }
    @Mutation
    set_art_class_tree(data: any) {
        // debugger
        for (let x of data) {
            this.ClassMap[x.CID] = x;
        }
        this.Tree = Object.values(array_tree(data, { pfield: 'PCID', ufield: 'CID', sub_name: 'children' }))
    }
    /**
     * 文章分类的map值
     */
    get art_class_map() {
        return this.ClassMap;
    }
    get art_result() {
        return this.Result;
    }
    @Action({ rawError: true })
    async get_art_list(where: SearchWhere) {
        let w = where || this.Where;
        this.context.commit('set_art_result', await Art.Art.list(where))
        return this.Result;
    }
    @Action({ rawError: true })
    async get_art_class_tree(where: any) {
        this.context.commit('set_art_class_tree', await Art.Classify.all(where || {}));
        return this.Result;
    }

}