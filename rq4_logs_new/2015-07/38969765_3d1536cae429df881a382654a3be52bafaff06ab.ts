/// <reference path="definitions/jquery.d.ts"/>
/// <reference path="definitions/google.maps.d.ts"/>
/// <reference path="AdhikamGlobal.ts"/>
/// <reference path="AdhikamModels.ts"/>

module AdhikamMap {

    export class ClusterController {
        private baseMarkers: google.maps.Marker[];
        private displayMarkers: ClusterMarker[];
        private clusterPolylines: google.maps.Polyline[];
        private pageListeners: google.maps.MapsEventListener[];
        private clusterImages: google.maps.MarkerImage[];

        constructor () {
            this.baseMarkers = new Array();
            this.displayMarkers = new Array();
            this.clusterPolylines = new Array();
            this.pageListeners = new Array();
            this.clusterImages = new Array();
            this.buildClusterImages();
        }

        public clearClusters(): void {
            console.log("propertiesDataController going to clear previous clusters ", this.baseMarkers.length);
            //clear markers
            if(this.baseMarkers.length !== 0){
                for (var i = 0; i < this.baseMarkers.length; i++) {
                    this.baseMarkers[i].setMap(<google.maps.Map>null);
                    this.displayMarkers[i].setMap(<google.maps.Map>null);
                    this.baseMarkers[i] = null;
                    this.displayMarkers[i] = null;
                }
            }
            for (var i = 0; i < this.clusterPolylines.length; i++) {
                this.clusterPolylines[i].setMap(<google.maps.Map>null);
                this.clusterPolylines[i] = null;
            }
            this.baseMarkers.length = 0;
            this.displayMarkers.length = 0;
            this.clusterPolylines.length = 0;
            //clear listeners
            if (this.pageListeners.length !== 0) {
                for (var i = 0; i < this.pageListeners.length; i++) {
                    google.maps.event.removeListener(this.pageListeners[i]);
                }
            }
        }

        public handleClusters(clusters: Cluster[]): void {
            console.log("propertiesDataController going to add clusters ", clusters);
            var clusterSize: number = jQuery.isEmptyObject(clusters) ? 0 : clusters.length;
            console.log("propertiesDataController going to add cluster size ", clusterSize);
            for (var i = 0; i < clusterSize; i++) {
                this.createClusterMarker(<Cluster>clusters[i]);
            }
            console.log("finished adding clusterMarkers ", this.displayMarkers.length);  
        }
        
        private createClusterMarker(cluster: Cluster): void {
            this.addClusterMarker(cluster);
            //this.addPolyline(cluster);
        }

        private addClusterMarker(cluster: Cluster): void {
            //create marker and cluster marker
            var marker: google.maps.Marker = new google.maps.Marker({
                position: new google.maps.LatLng(cluster.center.lat, cluster.center.lng), 
                map: AdhikamGlobal.appMap, icon: this.getClusterImage(cluster),  
                zIndex: 10
            });
            console.log("marker ", marker);
            this.baseMarkers.push(marker);
            var clusterMarker: ClusterMarker = new ClusterMarker({ map: AdhikamGlobal.appMap });
            clusterMarker.set('zIndex', 11);
            clusterMarker.bindTo('position', marker, 'position');
            clusterMarker.set('text', cluster.count);
            this.displayMarkers.push(clusterMarker);
            //create marker listeners
            this.pageListeners.push( google.maps.event.addListenerOnce(marker, 'click', () => { 
                console.log("boundingBox ", cluster.boundingBox);
                $("#detailsDiv").hide();
                AdhikamGlobal.appMap.fitBounds(
                    new google.maps.LatLngBounds(
                        new google.maps.LatLng(cluster.boundingBox.bottomRightLat, cluster.boundingBox.topLeftLng),
                        new google.maps.LatLng(cluster.boundingBox.topLeftLat, cluster.boundingBox.bottomRightLng)
                    )
                )}));
        }

        private addPolyline(cluster: Cluster): void {
            var boundingBoxPoints = [
            new google.maps.LatLng(cluster.boundingBox.topLeftLat, cluster.boundingBox.topLeftLng), 
            new google.maps.LatLng(cluster.boundingBox.topLeftLat, cluster.boundingBox.bottomRightLng),
            new google.maps.LatLng(cluster.boundingBox.bottomRightLat, cluster.boundingBox.bottomRightLng), 
            new google.maps.LatLng(cluster.boundingBox.bottomRightLat, cluster.boundingBox.topLeftLng), 
            new google.maps.LatLng(cluster.boundingBox.topLeftLat, cluster.boundingBox.topLeftLng)
            ];

            var boundingBox = new google.maps.Polyline({
                path: boundingBoxPoints,
                strokeColor: '#FF0000',
                strokeOpacity: 1.0,
                strokeWeight: 2
            });
            boundingBox.setMap(AdhikamGlobal.appMap);
            this.clusterPolylines.push(boundingBox);
        }
        /*
        private createClusterImage(cluster: Cluster): google.maps.MarkerImage {
            console.log("cluster ", cluster);
            var image: google.maps.MarkerImage = new google.maps.MarkerImage('assets/img/site/red-marker.png');
            image.size = new google.maps.Size(32, 32);
            image.anchor = new google.maps.Point(16, 16);
            if (cluster.count < 10) {
                image.scaledSize = new google.maps.Size(20, 20);
                image.anchor = new google.maps.Point(10, 10);
            } else if (cluster.count < 100) {
                image.scaledSize = new google.maps.Size(28, 28);
                image.anchor = new google.maps.Point(14, 14);
            } else if (cluster.count < 1000) {

            } else if (cluster.count < 10000) {
                image.size = new google.maps.Size(36, 36);
                image.scaledSize = new google.maps.Size(36, 36);
                image.anchor = new google.maps.Point(18, 18);
            } else {
                image.size = new google.maps.Size(42, 42);
                image.scaledSize = new google.maps.Size(42, 42);
                image.anchor = new google.maps.Point(21, 21);
            }
            console.log("image ", image);
            return image;
        }        
        */
        private getClusterImage(cluster: Cluster): google.maps.MarkerImage {
            if (cluster.count < 10) {
                return this.clusterImages[1];
            } else if (cluster.count < 100) {
                return this.clusterImages[2];
            } else if (cluster.count < 1000) {
                return this.clusterImages[3];
            } else if (cluster.count < 10000) {
                return this.clusterImages[4];
            } else {
                return this.clusterImages[5];
            }
        }

        private buildClusterImages(): void {
            var image: google.maps.MarkerImage = new google.maps.MarkerImage('assets/img/site/red-marker.png');
            image.scaledSize = new google.maps.Size(20, 20);
            image.anchor = new google.maps.Point(10, 10);
            this.clusterImages[1] = image;
            image = new google.maps.MarkerImage('assets/img/site/red-marker.png');
            image.scaledSize = new google.maps.Size(28, 28);
            image.anchor = new google.maps.Point(14, 14);
            this.clusterImages[2] = image;
            image = new google.maps.MarkerImage('assets/img/site/red-marker.png');
            image.size = new google.maps.Size(32, 32);
            image.anchor = new google.maps.Point(16, 16);
            this.clusterImages[3] = image;
            image = new google.maps.MarkerImage('assets/img/site/red-marker.png');
            image.size = new google.maps.Size(36, 36);
            image.scaledSize = new google.maps.Size(36, 36);
            image.anchor = new google.maps.Point(18, 18);
            this.clusterImages[4] = image;
            image = new google.maps.MarkerImage('assets/img/site/red-marker.png');
            image.size = new google.maps.Size(42, 42);
            image.scaledSize = new google.maps.Size(42, 42);
            image.anchor = new google.maps.Point(21, 21);
            this.clusterImages[5] = image;
        }
    }
}