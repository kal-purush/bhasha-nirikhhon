package org.openapitools.services.implementations;

import com.mongodb.client.model.geojson.Point;
import org.openapitools.model.Reporte;
import org.openapitools.model.enums.StatusReporte;
import org.openapitools.repositories.ReportRepository;
import org.openapitools.services.interfaces.ReporteServicio;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;

import java.time.LocalDateTime;
import java.util.List;

public class ReporteServicioImpl implements ReporteServicio {

    private final ReporteServicio reporteServicio;

    @Autowired
    public ReporteServiceImpl(ReportRepository reportRepository) {
        this.reportRepository = reportRepository;
    }

    @Override
    public Reporte guardarReporte(Reporte reporte) throws Exception {
        // Aquí podrías agregar validaciones o lógica adicional
        return reportRepository.save(reporte);
    }

    @Override
    public void eliminarReporte(String id) throws Exception {
        if (!reportRepository.existsById(id)) {
            throw new Exception("El reporte con id " + id + " no existe.");
        }
        reportRepository.deleteById(id);
    }

    @Override
    public Reporte obtenerReporte(String id) throws Exception {
        return reportRepository.findById(id)
                .orElseThrow(() -> new Exception("Reporte no encontrado con id: " + id));
    }

    @Override
    public List<Reporte> obtenerReportesPorUsuario(String idUsuario) throws Exception {
        return reportRepository.findByIdUsuarioOrderByFechaDesc(idUsuario);
    }

    @Override
    public List<Reporte> obtenerReportesPorUsuarioYEstado(String idUsuario, StatusReporte estado) throws Exception {
        return reportRepository.findByIdUsuarioAndEstado(idUsuario, estado);
    }

    @Override
    public List<Reporte> obtenerReportesImportantes(int contMinimo) throws Exception {
        return reportRepository.findByContImportanciaGreaterThanOrderByFechaDesc(contMinimo);
    }

    @Override
    public List<Reporte> obtenerReportesPorCategoria(String categoriaId) throws Exception {
        return reportRepository.findByCategorias_Id(categoriaId);
    }

    @Override
    public List<Reporte> obtenerReportesCercanos(Point ubicacion, Distance distancia) throws Exception {
        return reportRepository.findByUbicacionNear(ubicacion, distancia);
    }

    @Override
    public List<Reporte> obtenerReportesEntreFechas(LocalDateTime inicio, LocalDateTime fin) throws Exception {
        return reportRepository.findByFechaBetween(inicio, fin);
    }

    @Override
    public List<Reporte> obtenerReportesDespuesDe(LocalDateTime fecha) throws Exception {
        return reportRepository.findByFechaAfter(fecha);
    }

    @Override
    public List<Reporte> obtenerReportesPorRangoDeImportancia(int min, int max) throws Exception {
        return reportRepository.findByContImportanciaBetween(min, max);
    }

    @Override
    public long contarReportesEntreFechas(LocalDateTime inicio, LocalDateTime fin) throws Exception {
        return reportRepository.countByFechaBetween(inicio, fin);
    }
}