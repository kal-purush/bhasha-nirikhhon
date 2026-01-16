'use strict'

/**
 * @ngdoc overview
 * @name dnwebApp
 * @description
 * # dnwebApp
 *
 * Main module of the application.
 */

angular
  .module('dnwebApp', [
    'ngRaven',
    'ngAnimate',
    'ngSanitize',
    'ngResource',
    'ngTouch',
    'oitozero.ngSweetAlert',
    'ui.router',
    'ui.select',
    'ui.bootstrap',
    'uiGmapgoogle-maps',
    'angularMoment',
    'angular-loading-bar',
    'checklist-model',
    'kendo.directives',
    'ngFileUpload',
    'ragasa.notificaciones',
    'btford.socket-io',
    'ui-leaflet'
  ])
  .config(
    function ($stateProvider, $urlRouterProvider, uiGmapGoogleMapApiProvider,
      cfpLoadingBarProvider, recibirNotificacionesProvider,
      enviarNotificacionesProvider) {
      recibirNotificacionesProvider.defaults.servidor = 'http://192.6.22.27:3000/'
      enviarNotificacionesProvider.defaults.servidor = 'http://192.6.22.27:3000/'

      uiGmapGoogleMapApiProvider.configure({
        key: 'AIzaSyCx3WCABIljA9ngNhoTSAD_hIsXbzIrbHs'
      })

      cfpLoadingBarProvider.includeSpinner = false

      $urlRouterProvider.otherwise('/inicio')

      $stateProvider
        .state('inicio', {
          url: '/inicio',
          templateUrl: 'views/inicio.html',
          controller: 'InicioCtrl',
          loginRequired: true,
          validateRole: false,
          cache: false
        })
        .state('login', {
          url: '/login',
          templateUrl: 'views/login.html',
          controller: 'LoginCtrl',
          cache: false
        })
        .state('supervision', {
          url: '/supervision',
          templateUrl: 'views/supervision.html',
          controller: 'SupervisionCtrl',
          cache: false,
          loginRequired: true,
          validateRole: true
        })
        .state('supervision.supervision-usuario', {
          url: '/usuario/{idUsuario:int}/{idRuta:int}',
          views: {
            '@': {
              templateUrl: 'views/supervisionusuario.html',
              controller: 'SupervisionUsuarioCtrl'
            }
          },
          cache: false,
          loginRequired: true,
          validateRole: false,
          resolve: {
            miRuta: function ($stateParams, ruta) {
              return ruta.obtenerRutaDia($stateParams.idRuta)
                .then(function (ruta) {
                  return ruta
                })
            },
            miUsuario: function ($stateParams, usuario) {
              return usuario.obtenerUsuario($stateParams.idUsuario)
                .then(function (u) {
                  return u
                })
            }
          },
          params: {}
        })
        .state('alta-persona', {
          url: '/alta/persona',
          templateUrl: 'views/altapersona.html',
          controller: 'AltaPersonaCtrl',
          cache: false,
          loginRequired: true,
          params: {}
        })
        .state('organigrama', {
          url: '/organigrama',
          templateUrl: 'views/organigrama.html',
          controller: 'OrganigramaCtrl',
          cache: false,
          loginRequired: true,
          params: {}
        })
        .state('noautorizado', {
          url: '/noautorizado',
          templateUrl: 'views/noautorizado.html',
          controller: function ($scope) {
            $scope.setTitle('')
          }
        })
        .state('ruta-nueva', {
          url: '/alta/ruta',
          templateUrl: 'views/crearruta.html',
          controller: 'CrearRutaCtrl',
          cache: false,
          loginRequired: true,
          validateRole: true,
          params: {}
        })
        .state('alta-config-cuestionario', {
          url: '/alta/cuestionario',
          templateUrl: 'views/altaconfigcuestionario.html',
          controller: 'AltaConfigCuestionarioCtrl',
          cache: false,
          loginRequired: true,
          params: {}
        })
        .state('alta-config-cuestionario-version-2', {
          url: '/alta/cuestionariov2',
          templateUrl: 'views/altaconfigcuestionariov2.html',
          controller: 'AltaConfigCuestionariov2Ctrl',
          cache: false,
          loginRequired: true,
          params: {}
        })
        .state('reporte-cuestionario', {
          url: '/reporte/cuestionario',
          templateUrl: 'views/reportecuestionario.html',
          controller: 'ReportecuestionarioCtrl',
          cache: false,
          loginRequired: true,
          params: {}
        })
        .state('ruta-historico', {
          url: '/ruta/historico?ruta&{fecha:date}',
          templateUrl: 'views/historicoruta.html',
          controller: 'HistoricoRutaCtrl',
          cache: false,
          loginRequired: true,
          params: {}
        })
        .state('ruta-dia', {
          url: '/ruta/dia?ruta',
          templateUrl: 'views/rutadia.html',
          controller: 'RutaDiaCtrl',
          cache: true,
          loginRequired: true,
          params: {}
        })
        .state('detalle-visita', {
          url: '/visita/{idVisita:int}',
          templateUrl: 'views/detallevisita.html',
          controller: 'DetalleVisitaCtrl',
          cache: false,
          loginRequired: true,
          validateRole: false,
          params: {
            idVisita: 0
          }
        })

        .state('reporte-ruta-administrativo', {
          url: '/reporte/reporterutaadministrativo',
          templateUrl: 'views/reporterutaadministrativo.html',
          controller: 'reporteRutaAdministrativoCtrl',
          cache: false,
          loginRequired: true,
          validateRole: false,
          params: {
            idVisita: 0
          }
        })

        .state('detalle-visita.cuestionario', {
          url: '/{nombre}/{idHistorico}',
          views: {
            detalle: {
              templateUrl: function (params) {
                return 'views/cuestionarios/' + params.nombre + '.html'
              },
              controller: 'DetalleCuestionarioCtrl'
            }
          },
          cache: false,
          loginRequired: true,
          validateRole: false,
          params: {}
        })
        .state('auditar-cuestionarios', {
          url: '/auditar/cuestionarios',
          templateUrl: 'views/reportecuestionario.html',
          controller: 'ReportecuestionarioCtrl',
          cache: false,
          loginRequired: true,
          params: {}
        })
        .state('alta-tarea', {
          url: '/alta/tarea',
          templateUrl: 'views/altatarea.html',
          controller: 'AltaTareaCtrl',
          cache: false,
          loginRequired: true,
          params: {}
        })
        .state('alta-actividad', {
          url: '/alta/actividad',
          templateUrl: 'views/altatarea.html',
          controller: 'AltaTareaCtrl',
          cache: false,
          loginRequired: true,
          params: {}
        })
        .state('justificaciones', {
          url: '/justificaciones',
          templateUrl: 'views/justificacion.html',
          controller: 'JustificacionesCtrl',
          cache: false,
          loginRequired: true,
          validateRole: false,
          params: {}
        })
        .state('rutas', {
          url: '/rutas',
          templateUrl: 'views/listadorutas.html',
          controller: 'ListadoRutasCtrl',
          cache: false,
          loginRequired: true,
          validateRole: false,
          params: {}
        })
        .state('justificaciones.justificacionesalta', {
          url: '/altajustificacion',
          views: {
            '@': {
              templateUrl: 'views/justificacion-alta.html',
              controller: 'JustificacionAltaCtrl'
            }
          },
          cache: false,
          loginRequired: true,
          validateRole: false
        })
        .state('rutas.nueva', {
          url: '/alta',
          views: {
            '@': {
              templateUrl: 'views/crearruta.html',
              controller: 'CrearRutaCtrl'
            }
          },
          cache: false,
          loginRequired: true,
          validateRole: false,
          params: {}
        })
        .state('rutas.editar', {
          url: '/editar/{idruta:int}',
          views: {
            '@': {
              templateUrl: 'views/crearruta.html',
              controller: 'CrearRutaCtrl'
            }
          },
          cache: false,
          loginRequired: true,
          validateRole: false
        })
        .state('reporte-justificaciones', {
          url: '/reporte/justificaciones',
          templateUrl: 'views/reportejustificaciones.html',
          controller: 'ReporteJustificacionesCrtl',
          cache: false,
          loginRequired: false,
          params: {}
        })
        .state('reporte-ingreso-usuarios-movil', {
          url: '/reporte/usuarios/movil',
          templateUrl: 'views/reporteusuariosnoingresomovil.html',
          controller: 'ReporteIngresoUsuariosMovilCtrl',
          cache: false,
          validateRole: false,
          loginRequired: false,
          params: {}
        })
        .state('reporte-ingreso-ejecutivos', {
          url: '/reporte/ingreso/ejecutivos',
          templateUrl: 'views/reporteingresoejecutivos.html',
          controller: 'ReporteIngresoEjecutivosCtrl',
          cache: false,
          validateRole: false,
          loginRequired: false,
          params: {}
        })
        .state('reporte-conteo-ingreso-ejecutivos', {
          url: '/reporte/conteo/ingreso/ejecutivos',
          templateUrl: 'views/reporteconteoingresoejecutivos.html',
          controller: 'ReporteConteoIngresoEjecutivosCtrl',
          cache: false,
          validateRole: false,
          loginRequired: false,
          params: {}
        })
        .state('reporte-usuarios-sesion-activa', {
          url: '/reporte/usuarios/sesion/activa',
          templateUrl: 'views/reporteusuariossesionactiva.html',
          controller: 'ReporteUsuariosSesionActivaCtrl',
          cache: false,
          validateRole: false,
          loginRequired: false,
          params: {}
        })
    }
  )
  .run(function ($rootScope, $location, configuracion, autenticacion, $http,
    $state, opciones, SweetAlert, DNConfig, recibirNotificaciones) {
    moment.locale('es')

    Raven.setEnvironment(configuracion.DireccionActiva.match(/ragasaapps/) ? 'production' : 'development')
    Raven.setRelease(configuracion.version)

    if (window.Storage) {
      window.Storage.prototype.setObject = function (key, obj) {
        this.setItem(configuracion.IDSistema + '_' + key, JSON.stringify(obj))
      }

      window.Storage.prototype.getObject = function (key) {
        var value = this.getItem(configuracion.IDSistema + '_' + key)
        var obj = value === null ? null : JSON.parse(value)
        return obj
      }
    }

    if (autenticacion.isLogged()) {
      var usuarioActivo = DNConfig.getUsuarioActivo()
      if (usuarioActivo && usuarioActivo.Usuario) {
        recibirNotificaciones.suscribir(usuarioActivo.Usuario)
      }
    }

    $rootScope.$on('socket:nueva-notificacion', function (ev, notificacion) {
      var toastr = window.toastr
      var options = {
        closeButton: true,
        debug: false,
        newestOnTop: false,
        progressBar: false,
        positionClass: 'toast-top-right',
        preventDuplicates: false,
        onclick: null,
        showDuration: '300',
        hideDuration: '1000',
        timeOut: '0',
        extendedTimeOut: '0',
        showEasing: 'swing',
        hideEasing: 'linear',
        showMethod: 'fadeIn',
        hideMethod: 'fadeOut'
      }
      toastr.info(notificacion.mensaje, notificacion.remitente + ' - ' + notificacion.titulo, options)
    })

    $http.defaults.headers.common = autenticacion.getLoginHeaders()

    $rootScope.$on('$stateChangeError', function () {
      var detalle = _.get(arguments, '5.data.Mensaje', '')
      console.log(arguments[5])
      SweetAlert.error('Error', 'Ha ocurrido un error al inicializar esta vista.\n\n' + detalle)
    })

    $rootScope.$on('$stateChangeSuccess', function (event, toState) {
      Raven.setExtraContext({
        url: $location.url()
      })
      Raven.setExtraContext(toState)
    })

    $rootScope.$on('$stateChangeStart', function (event, toState) {
      var usuario = DNConfig.getUsuarioActivo()
      if (autenticacion.isLogged() && usuario) {
        Raven.setUserContext({
          user: usuario.Usuario,
          user_id: usuario.ID,
          id: usuario.Usuario
        })
      } else {
        Raven.setUserContext()
      }

      if (toState.url === '/login' && autenticacion.isLogged()) {
        event.preventDefault()
        $location.path('/inicio')
        return
      }

      if (toState.loginRequired && !autenticacion.isLogged()) {
        event.preventDefault()
        $rootScope.returnToState = toState.url
        $state.go('login')
      }

      if (toState.loginRequired && toState.validateRole !== false) {
        if (!opciones.isInRole(toState.name)) {
          event.preventDefault()
          $state.go('noautorizado')
        }
      }
    })

    /* moment.fn.microsoftDate = function() {
         return '/Date(' + this.valueOf() + '-0700)/';
      }; */
  })