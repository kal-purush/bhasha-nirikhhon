export const template = `
    <nav class="navbar navbar-inverse">
        <div class="container-fluid">

            <div class="navbar-header">
                <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#bs-example-navbar-collapse-1" aria-expanded="false">
                    <span class="sr-only">Toggle navigation</span>
                    <span class="icon-bar"></span>
                    <span class="icon-bar"></span>
                    <span class="icon-bar"></span>
                </button>
                <a class="navbar-brand" href="#"><span class="glyphicon glyphicon-cloud"></span> Weather App</a>
            </div>

            <div class="collapse navbar-collapse" id="bs-example-navbar-collapse-1">
                <ul class="nav navbar-nav">

                </ul>
            </div>
        </div>
    </nav>
    <div class="container">
        <div id="loader-box" class="text-center"><div class="loader"></div></div>
          <div class="row">
            <cards-list></cards-list>
          </div>
          <div class="row"><div class="col-xs-12">
            <google-map id="map"></google-map>
          </div>
        </div>
    </div>
`;