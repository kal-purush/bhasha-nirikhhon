/**
 * Created by vigon on 03/10/2017.
 */


module mathis{


    export module general_algo{


        export class UnionFind<T extends HasHashString>{

            private parents=new HashMap<T,T>(true)
            private ranks=new HashMap<T,number>()

            /**
             * On effectue une recherche dans l'arbre pour savoir qui est le parent de x
             Au passage : tous les noeuds entre x et parent[x] sont directement attaché à parent[x]
             (on aplatit l'arbre au maximum)
             * */
            find(x:T):T{
                this.addElement(x)
                return this.findRec(x)
            }

            private findRec(x:T):T{
                while (this.parents.getValue(x)!=this.parents.getValue(this.parents.getValue(x))) {
                    this.parents.putValue(x,this.findRec(this.parents.getValue(x)))
                }
                return this.parents.getValue(x)
            }

            union(x:T,y:T):boolean{

                let xParent=this.find(x)
                let yParent=this.find(y)

                if (xParent.hashString==yParent.hashString) return false

                if (this.ranks.getValue(xParent)<this.ranks.getValue(yParent) ){
                    this.parents.putValue(xParent,yParent)
                }
                else if (this.ranks.getValue(xParent)>this.ranks.getValue(yParent) ){
                    this.parents.putValue(yParent,xParent)
                }
                else{
                    this.parents.putValue(xParent,yParent)
                    let y_oldRank=this.ranks.getValue(yParent)
                    this.ranks.putValue(yParent,y_oldRank+this.ranks.getValue(xParent)+1)
                }

                return true

            }


            addElement(x:T){
                if (this.parents.getValue(x)==null){
                    this.parents.putValue(x,x)
                    this.ranks.putValue(x,0)
                }
            }



            get_components(addRep=true):HashMap<T,T[]>{

                let res=new HashMap<T,T[]>(true)

                let all=this.parents.allKeys()

                for (let v of all){
                    if (this.find(v).hashString==v.hashString) {
                        if (addRep) res.putValue(v,[v])
                        else res.putValue(v,[])
                    }
                }

                for (let v of all){
                    let rep=this.find(v)
                    if (rep.hashString!=v.hashString) {
                        res.getValue(rep).push(v)
                    }
                }

                return res
            }





        }




    }

}