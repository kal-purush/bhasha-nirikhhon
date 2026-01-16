/**
 * Created by Kieffer on 23/04/2017.
 */


module mathis {

    export module surfaceConnection {

        export class SurfaceConnectionProcess {
            makeLinks = true
            mamesh: Mamesh

            constructor(mamesh: Mamesh) {
                this.mamesh = mamesh
            }

            go(): Mamesh {

                /** *********************** **/
                /** Calcul des normales **/
                /** *********************** **/

                let tabRetenuVect1 = [];
                let tabRetenuVect2 = [];

                /** création de la HashMap **/
                let vertexToNormal=new HashMap<Vertex,XYZ>()
                let vertexToNumStrat=new HashMap<Vertex,number>()

                /** Calcul des produits scalaires. On parcourt chaque point **/
                for (let v of this.mamesh.vertices){

                    /** initialisation des variables */
                    let minscal = 0;
                    let instantDot;
                    let vectorTable = [];

                    /** Pour chaque lien **/
                    for (let l of v.links){

                        /** creation d'un vecteur à l'aide des coordonnés**/
                        let vect = new BABYLON.Vector3(0,0,0);
                        vect.x = l.to.param.x - v.param.x;
                        vect.y = l.to.param.y - v.param.y;
                        vect.z = l.to.param.z - v.param.z;
                        vect.normalize();

                        /** on récupère les vecteurs normalisés de chaque lien dans une table**/
                        vectorTable.push(vect);
                    } //ffor pour chaque lien


                    /** calcul du scalaire minimum : Initiatlisation **/
                    minscal = BABYLON.Vector3.Dot(vectorTable[0], vectorTable[1]);
                    if (minscal < 0) minscal *= -1; //valeur absolue
                    let vect1 = vectorTable[0]; let vect2 = vectorTable[1];

                    /** Double boucle pour chaque vecteur comparé à chaque autres vecteur (dot product)**/
                    for (let i = 0; i < vectorTable.length; i++) {
                        for (let j = i+1; j < vectorTable.length; j++) {
                            instantDot = BABYLON.Vector3.Dot(vectorTable[i], vectorTable[j]);
                            if (instantDot < 0) instantDot *= -1; //valeur absolue

                            /** si on trouve une valeur plus petite **/
                            if (minscal > instantDot)
                            {
                                /** nouveau minimum **/
                                minscal = instantDot;
                                /** nouveaux vecteurs retenus pour le crossProduct **/
                                vect1 = vectorTable[i]; vect2 = vectorTable[j];
                            }

                        }
                    }
                    /** Tableaux où on stocke les vecteurs retenus (un pour tous les 1er vecteurs de tous les points, **/
                    /** un autre tous les 2eme vecteurs de tous les points) **/
                    tabRetenuVect1.push(vect1);
                    tabRetenuVect2.push(vect2);

                    vertexToNumStrat.putValue(v,-1)
                } //ffor pour chaque point

                /** calcul de la normale **/
                for (let i = 0; (i < tabRetenuVect1.length) && ( i < tabRetenuVect2.length); i++) {
                    let normale = BABYLON.Vector3.Cross(tabRetenuVect1[i], tabRetenuVect2[i]);
                    normale.normalize();

                    /** enregistrement de la normale dans la HashMap **/
                    vertexToNormal.putValue(this.mamesh.vertices[i],new XYZ(normale.x,normale.y,normale.z))
                }


                /** *********************** **/
                /** Alignement des normales **/
                /** *********************** **/

                /** choix du vertex de départ **/
                let startvertex = this.mamesh.vertices[0];
                let i = 0;

                /** si normale nulle, prend un autre point **/
                while (i < this.mamesh.vertices.length -1 && vertexToNormal.getValue(startvertex).x == 0 && vertexToNormal.getValue(startvertex).y == 0 && vertexToNormal.getValue(startvertex).z == 0) {
                    startvertex = this.mamesh.vertices[i+1];
                    i++;
                } //todo: si tous les points de la figure ont une normale nulle, indiquer une erreur: figure invalide


                /** initatlisation des strates **/
                let markedVertex = [];
                markedVertex.push(startvertex);
                let strates = [];
                strates.push(markedVertex);
                let alreadySeen = new mathis.HashMap<Vertex,boolean>()

                let curentEdge = markedVertex;
                while (curentEdge.length > 0) {
                    curentEdge = mathis.graph.getEdge(curentEdge, alreadySeen);
                    strates.push(curentEdge);
                }

                /** parcours des strates **/
                for (let i = 1; i < strates.length; i++){
                    for (let v of strates[i]){
                        /**pour tous les links du point **/
                        for (let l of v.links){
                            /**pour tous les points de la strate précédente **/
                            for (let p of strates[i-1]){
                                /** s'ils sont adjacents **/
                                if(l.to == p){
                                    /** si normale nulle, prend la normale précédente **/
                                    if (vertexToNormal.getValue(v).x == 0 && vertexToNormal.getValue(v).y == 0 && vertexToNormal.getValue(v).z == 0)
                                    {
                                        let newNormale0 = new XYZ(vertexToNormal.getValue(p).x, vertexToNormal.getValue(p).y, vertexToNormal.getValue(p).z)
                                        vertexToNormal.putValue(v,newNormale0)
                                    } //fnormale_nulle

                                    /** inverser la normale si le dot product entre les normales est négatif **/
                                    if (BABYLON.Vector3.Dot(vertexToNormal.getValue(p), vertexToNormal.getValue(v)) < 0)
                                    {
                                        let newNormale = new XYZ(-vertexToNormal.getValue(v).x, -vertexToNormal.getValue(v).y, -vertexToNormal.getValue(v).z)
                                        vertexToNormal.putValue(v,newNormale)
                                    } //finversion
                                } //flink<>p
                            } //fstrate-1
                        } //flinks
                    } //fvertex
                } //fstrats

                /** detection de polygones **/

                let tab_polys = []
                let dejaParcours = new StringMap<boolean>()

                /** creation d'un tableau de liens **/
                let tablinks = []
                for (let v of this.mamesh.vertices)
                {
                    for (let l of v.links)
                    {
                        /** initialisation dejaParcours, tous les liens à faux**/
                        dejaParcours.putValue(v.hashNumber + "," + l.to.hashNumber, false)
                        let elemlink = [v,l.to]
                        tablinks.push(elemlink)
                    }
                }

                /** pour chaque link **/
                for (let lk of tablinks) {

                    /** initialisation vecteur et lien de depart **/
                    let first_point = lk[0]
                    let v_previous = lk[0]
                    let v_current = lk[1]

                    /** si le lien de depart est déjà parcouru, on le zap **/
                    if (dejaParcours.getValue(v_previous.hashNumber + "," + v_current.hashNumber) != true)
                    {
                        /** detection d'un premier polygone **/
                        let current_poly = [v_current]

                        /** while experimental, sur k < n étapes max **/
                        let vloop = true
                        while (vloop) {
                            /** initialisation du minimum **/
                            let anglemin = 6.4
                            let l_pretend = v_previous.links[0]

                            /** pour chaque lien du vertex **/
                            for (let l_concur of v_current.links) {
                                /** on omet le lien de départ et les liens déjà parcourus**/
                                if (l_concur.to != v_previous && dejaParcours.getValue(v_current.hashNumber + "," + l_concur.to.hashNumber) != true) {
                                    let vector_l_previous = new XYZ(v_previous.param.x - v_current.param.x, v_previous.param.y - v_current.param.y, v_previous.param.z - v_current.param.z)
                                    let vector_l_next = new XYZ(l_concur.to.param.x - v_current.param.x, l_concur.to.param.y - v_current.param.y, l_concur.to.param.z - v_current.param.z)
                                    /** calcul de l'angle **/
                                    let newangle = geo.angleBetweenTwoVectorsBetweenMinusPiAndPi(vector_l_previous, vector_l_next, vertexToNormal.getValue(v_previous))
                                    /** valeur absolue **/
                                    if (newangle < 0) newangle = (6.2831853072 + newangle)

                                    /** retient le nouvel angle et nouveau lien **/
                                    if (newangle < anglemin) {
                                        anglemin = newangle
                                        l_pretend = l_concur
                                    }
                                }
                            }

                            /** ajout du nouveau point dans le polygone **/
                            current_poly.push(l_pretend.to)

                            /** marquer le lien comme étant deja parcouru **/
                            dejaParcours.putValue(v_current.hashNumber + "," + l_pretend.to.hashNumber, true)

                            /** on passe au nouveau point **/
                            v_previous = v_current
                            v_current = l_pretend.to

                            /** on retombe sur le premier point, fin de detection du polygone **/
                            if (v_current == first_point) {
                                /** break **/
                                vloop = false
                            }
                        }
                        /** ajout d'un nouveau polygone dans la table **/
                        tab_polys.push(current_poly)
                    }
                }

                /** calcul des aires **/
                let tabSurface = []

                /** stoker la valeur de centre de polygone à plus de 4 sommets **/
                let PolygoIndexToVertexCenter=[]

                let oneOverLength = 0
                let polyIndex = -1
                for (let p of tab_polys)
                {
                    polyIndex ++
                    /** surface à 3 sommets **/
                    if (p.length == 3)
                    {
                        let distA = geo.distance(p[0].param, p[1].param)
                        let distB = geo.distance(p[0].param, p[2].param)
                        let distC = geo.distance(p[1].param, p[2].param)

                        /** calcul d'aire d'après la formule de Héron **/
                        let heronP = (distA + distB + distC) / 2
                        let surface =  Math.sqrt(heronP * (heronP - distA) * (heronP - distB) * (heronP - distC))
                        tabSurface.push(surface)
                    }
                    /** surface à 4 sommets **/
                    else if (p.length == 4)
                    {
                        let distA = geo.distance(p[0].param, p[1].param)
                        let distB = geo.distance(p[1].param, p[2].param)
                        let distC = geo.distance(p[2].param, p[3].param)
                        let distD = geo.distance(p[3].param, p[0].param)
                        let distE = geo.distance(p[0].param, p[2].param)

                        /** calcul d'aire d'après la formule de Héron **/
                        let heronP1 = (distA + distB + distE) / 2
                        let surface1 =  Math.sqrt(heronP1 * (heronP1 - distA) * (heronP1 - distB) * (heronP1 - distE))
                        let heronP2 = (distC + distD + distE) / 2
                        let surface2 =  Math.sqrt(heronP2 * (heronP2 - distC) * (heronP2 - distD) * (heronP2 - distE))

                        tabSurface.push(surface1 + surface2)
                    }
                    /** surface a plus de 4 sommets **/
                    else if (p.length >= 5) {
                        let centerVertex = new mathis.Vertex().setPosition(0, 0, 0);

                        /** Initialization **/
                        oneOverLength = 1 / (p.length)
                        let tab1 = [p[0].position]
                        let tab2 = [oneOverLength]

                        /** For each face **/
                        for (let v of p) {
                            tab1.push(v.position)
                            tab2.push(oneOverLength)
                        }
                        geo.baryCenter(tab1, tab2, centerVertex.param)

                        let centerVertex2 = new mathis.Vertex().setPosition(centerVertex.param.x, centerVertex.param.y, centerVertex.param.z);

                        let surface3 = 0
                        for (let i = 0; i < p.length; i++)
                        {
                            /** calcul d'aire d'après la formule de Héron **/
                            let distA = geo.distance(p[i].param, p[(i + 1) % (p.length)].param)
                            let distB = geo.distance(p[(i + 1) % (p.length)].param, centerVertex2.param)
                            let distC = geo.distance(centerVertex2.param, p[i].param)
                            let heronP3 = (distA + distB + distC) / 2
                            surface3 = surface3 + Math.sqrt(heronP3 * (heronP3 - distA) * (heronP3 - distB) * (heronP3 - distC))
                        }
                        tabSurface.push(surface3)
                        /** on stock la valeur du centre de gravité dui polygone pour éviter de la recalculer après **/
                        PolygoIndexToVertexCenter[polyIndex]=centerVertex2
                    }
                }

                /** trouver la surface la plus grande pour la supprimer (fait la totalité de la figure) **/
                let maxSurf = -1
                let numSurf = -1

                /** si le polygone n'a qu'une seule surface on ne va pas s'amuser à l'enlever **/
                if (tabSurface.length != 1)
                {
                    for (let i = 0; i < tabSurface.length; i++)
                    {
                        if (tabSurface[i] > maxSurf)
                        {
                            maxSurf = tabSurface[i]
                            numSurf = i
                        }
                    }
                }

                /** affichage des polygones **/
                let indexSurface = -1

                for (let p of tab_polys)
                {
                    indexSurface ++
                    /** surface à 3 sommets **/
                    if (p.length == 3 && indexSurface != numSurf)
                    {
                        this.mamesh.addATriangle(p[0], p[1], p[2])
                    }
                    /** surface à 4 sommets **/
                    else if (p.length == 4 && indexSurface != numSurf)
                    {
                        this.mamesh.addASquare(p[0], p[1], p[2], p[3])
                    }
                    /** surface a plus de 4 sommets **/
                    else if (p.length >= 5 && indexSurface != numSurf)
                    {
                        let centerVertex2 = PolygoIndexToVertexCenter[indexSurface]
                        centerVertex2.markers.push(Vertex.Markers.pintagoneCenter)
                        this.mamesh.vertices.push(centerVertex2)

                        for (let i = 0; i < p.length; i++)
                        {
                            this.mamesh.addATriangle(p[i], p[(i + 1) % (p.length)], centerVertex2)
                            p[i].setOneLink(centerVertex2)
                            centerVertex2.setOneLink(p[i])
                        }
                    }
                }
                return this.mamesh
            }
        }
    }
}